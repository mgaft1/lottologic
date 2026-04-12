"""
app.py  --  Lotto Viewer Phase 1
Window slicing by calendar days. Mode A navigation only.
PB/PD: effective_days = round(days * 0.6).
Background scraper fills missing draws; never blocks rendering.
"""

import logging
import os
import sys
import threading
from itertools import product
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# db_forecast and forecast live in ../data/ relative to this file (files/app.py).
# Resolve the path so local launches like `py app.py` don't accidentally
# look for a non-existent `files\\data` folder.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "data"))

from flask import Flask, render_template, request, jsonify, session, redirect, url_for, make_response
import db
import db_forecast
import db_selection
import db_links
import db_ticket_sim
import selection
import scraper
import gap_engine
import links_fetcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
APP_BUILD = (os.environ.get("RENDER_GIT_COMMIT") or os.environ.get("COMMIT_SHA") or "local")[:7]

app = Flask(__name__)
app.secret_key = os.environ.get("LOTTO_SECRET", "change-me-in-production-32chars!!")
_runtime_init_lock = threading.Lock()
_runtime_initialized = False

# ---------------------------------------------------------------------------
# Credentials  (set via environment variables; fallback for dev only)
# ---------------------------------------------------------------------------
AUTH_USER = os.environ.get("LOTTO_USER", "admin")
AUTH_PASS = os.environ.get("LOTTO_PASS", "lotto123")


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

from functools import wraps

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            if request.path.startswith("/api/"):
                return jsonify({"error": "Session expired. Please sign in again, then retry."}), 401
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return decorated

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WINDOW_DAYS   = [175, 350, 525, 700]
DEFAULT_DAYS  = 350

# PB and PD use scaled day lookback: round(points * 0.6)
PB_PD_SCALE   = 0.6
PB_PD_TYPES   = {"PB", "PD"}

LOTTO_LABELS = {
    "CA": "California Lotto",
    "FL": "Florida Lotto",
    "MM": "Mega Millions",
    "PB": "Powerball",
    "PD": "Powerball Double",
}

TICKET_GAME_RULES = {
    "CA": {"main_count": 5, "main_max": 47, "bonus_max": 27, "base_price": 1.0},
    "FL": {"main_count": 6, "main_max": 53, "bonus_max": None, "base_price": 2.0},
    "MM": {"main_count": 5, "main_max": 70, "bonus_max": 24, "base_price": 5.0},
    "PB": {"main_count": 5, "main_max": 69, "bonus_max": 26, "base_price": 2.0},
    "PD": {"main_count": 5, "main_max": 69, "bonus_max": 26, "base_price": 1.0},
}

FIXED_PRIZE_TABLES = {
    "PB": {
        (5, True): 0,
        (5, False): 1_000_000,
        (4, True): 50_000,
        (4, False): 100,
        (3, True): 100,
        (3, False): 7,
        (2, True): 7,
        (1, True): 4,
        (0, True): 4,
    },
    "PD": {
        (5, True): 10_000_000,
        (5, False): 500_000,
        (4, True): 50_000,
        (4, False): 500,
        (3, True): 500,
        (3, False): 20,
        (2, True): 20,
        (1, True): 10,
        (0, True): 7,
    },
}

def _resolve_app_timezone():
    tz_name = os.environ.get("LOTTO_TIMEZONE", "America/Los_Angeles")
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        logger.warning("Time zone %s not found; falling back to local system time", tz_name)
        return None


APP_TIMEZONE = _resolve_app_timezone()
TICKET_CUTOFF_TIME = time(19, 45)
DRAW_RESULTS_READY_TIME = time(21, 30)
VIEWER_REFRESH_RETRY_WINDOW = timedelta(minutes=10)
_viewer_refresh_attempts: dict[str, datetime] = {}
_viewer_refresh_state_lock = threading.Lock()
_viewer_refresh_locks = {lt: threading.Lock() for lt in LOTTO_LABELS}
TICKET_CLEANUP_INTERVAL = timedelta(minutes=30)
_ticket_cleanup_lock = threading.Lock()
_ticket_cleanup_last_run: datetime | None = None


def _is_render_runtime() -> bool:
    return bool(os.environ.get("RENDER")) or "render.com" in os.environ.get("RENDER_EXTERNAL_URL", "")


def _env_flag(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _now_local() -> datetime:
    return datetime.now(APP_TIMEZONE) if APP_TIMEZONE else datetime.now()


def trigger_ticket_cleanup_async(force: bool = False) -> None:
    """Keep ticket cleanup off the critical request path."""
    global _ticket_cleanup_last_run

    now_local = _now_local()
    if not force and _ticket_cleanup_last_run and (now_local - _ticket_cleanup_last_run) < TICKET_CLEANUP_INTERVAL:
        return
    if _ticket_cleanup_lock.locked():
        return

    def _cleanup_job() -> None:
        global _ticket_cleanup_last_run
        if not _ticket_cleanup_lock.acquire(blocking=False):
            return
        try:
            deleted = db_ticket_sim.purge_expired_tickets()
            _ticket_cleanup_last_run = _now_local()
            if deleted:
                logger.info("Ticket cleanup removed %d expired ticket(s)", deleted)
        except Exception as exc:
            logger.warning("Ticket cleanup failed: %s", exc)
        finally:
            _ticket_cleanup_lock.release()

    threading.Thread(target=_cleanup_job, name="ticket-cleanup", daemon=True).start()


# ---------------------------------------------------------------------------
# Window helpers
# ---------------------------------------------------------------------------

def effective_days(lotto_type: str, days: int) -> int:
    """Apply PB/PD scaling if applicable."""
    if lotto_type in PB_PD_TYPES:
        return round(days * PB_PD_SCALE)
    return days


def cutoff_past(cutoff_now: date, days: int) -> date:
    return cutoff_now - timedelta(days=days)


def parse_date_arg(s: str | None, fallback: date) -> date:
    if not s:
        return fallback
    try:
        return date.fromisoformat(s)
    except ValueError:
        return fallback


def next_scheduled_draw(lotto_type: str, last_draw: date) -> date:
    dow = last_draw.weekday()  # Mon=0
    if lotto_type in {"CA", "FL"}:
        days = {0: 2, 1: 1, 2: 3, 3: 2, 4: 1, 5: 4, 6: 3}
    elif lotto_type == "MM":
        days = {0: 1, 1: 3, 2: 2, 3: 1, 4: 4, 5: 3, 6: 2}
    else:
        days = {0: 2, 1: 1, 2: 3, 3: 2, 4: 1, 5: 3, 6: 2}
    return last_draw + timedelta(days=days[dow])


def is_draw_day(lotto_type: str, target: date) -> bool:
    dow = target.weekday()  # Mon=0
    if lotto_type in {"CA", "FL"}:
        return dow in {2, 5}  # Wed, Sat
    if lotto_type == "MM":
        return dow in {1, 4}  # Tue, Fri
    return dow in {0, 2, 5}   # PB, PD: Mon, Wed, Sat


def latest_completed_draw_date(lotto_type: str, now_local: datetime | None = None) -> date:
    """
    Return the most recent scheduled draw date that should reasonably be
    available by now for the given lotto type.
    """
    now_local = now_local or _now_local()
    candidate = now_local.date()
    if is_draw_day(lotto_type, candidate) and now_local.time() < DRAW_RESULTS_READY_TIME:
        candidate -= timedelta(days=1)
    while not is_draw_day(lotto_type, candidate):
        candidate -= timedelta(days=1)
    return candidate


def _refresh_forecasts_for_lotto(lotto_type: str) -> None:
    from forecast import backfill_predictions

    last = db_forecast.get_last_forecast_date(lotto_type, FORECAST_MODEL)
    if last is None:
        dates = db_forecast.get_draw_dates(lotto_type)
        if not dates:
            return
        logger.info("%s forecast missing; backfilling %d date(s)", lotto_type, len(dates))
        backfill_predictions(lotto_type, dates, FORECAST_MODEL, _dal=db_forecast)
        return

    new_dates = [d for d in db_forecast.get_draw_dates_after(lotto_type, last) if d > last]
    if new_dates:
        logger.info("%s forecast stale; backfilling %d new date(s)", lotto_type, len(new_dates))
        backfill_predictions(lotto_type, new_dates, FORECAST_MODEL, _dal=db_forecast)


def ensure_lotto_draws_current(lotto_type: str) -> None:
    """
    Viewer stale-data guard. If the DB's latest draw date is behind the most
    recent completed scheduled draw for this lotto type, attempt a targeted
    refresh before serving data.
    """
    now_local = _now_local()
    expected_latest = latest_completed_draw_date(lotto_type, now_local)
    _, latest_str = db.get_date_bounds(lotto_type)
    latest_db = date.fromisoformat(latest_str) if latest_str else None
    if latest_db and latest_db >= expected_latest:
        return

    with _viewer_refresh_state_lock:
        last_attempt = _viewer_refresh_attempts.get(lotto_type)
        if last_attempt and (now_local - last_attempt) < VIEWER_REFRESH_RETRY_WINDOW:
            logger.info(
                "%s stale-data refresh skipped; last attempt at %s within retry window",
                lotto_type,
                last_attempt.isoformat(),
            )
            return
        _viewer_refresh_attempts[lotto_type] = now_local

    lock = _viewer_refresh_locks[lotto_type]
    if lock.locked():
        logger.info("%s stale-data refresh already in progress", lotto_type)
        return

    def _refresh_job() -> None:
        if not lock.acquire(blocking=False):
            return
        try:
            _, latest_str = db.get_date_bounds(lotto_type)
            latest_db = date.fromisoformat(latest_str) if latest_str else None
            if latest_db and latest_db >= expected_latest:
                return

            logger.info(
                "%s latest draw stale on viewer request: db=%s expected=%s; refreshing in background",
                lotto_type,
                latest_db.isoformat() if latest_db else "none",
                expected_latest.isoformat(),
            )
            refresh_summary = scraper.refresh_lotto_type(lotto_type)
            logger.info("%s on-demand refresh summary: %s", lotto_type, refresh_summary)

            refresh_targets = [lotto_type]
            if lotto_type in {"PB", "PD"}:
                refresh_targets = ["PB", "PD"]
            for target in refresh_targets:
                _refresh_forecasts_for_lotto(target)
        except Exception as exc:
            logger.warning("%s on-demand viewer refresh failed: %s", lotto_type, exc)
        finally:
            lock.release()

    threading.Thread(
        target=_refresh_job,
        name=f"viewer-refresh-{lotto_type.lower()}",
        daemon=True,
    ).start()


def default_ticket_draw_date(lotto_type: str, latest_draw: date) -> date:
    now_local = datetime.now(APP_TIMEZONE) if APP_TIMEZONE else datetime.now()
    today = now_local.date()
    if today > latest_draw and is_draw_day(lotto_type, today) and now_local.time() < TICKET_CUTOFF_TIME:
        return today
    return next_scheduled_draw(lotto_type, latest_draw)


def _ticket_numbers_from_row(row: dict) -> list[int]:
    nums = [row["Nbr1"], row["Nbr2"], row["Nbr3"], row["Nbr4"], row["Nbr5"]]
    if row.get("Nbr6") is not None:
        nums.append(row["Nbr6"])
    return nums


def _parse_purchased_flag(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "purchased"}
    return True


def normalize_ticket_numbers(lotto_type: str, numbers: list[int]) -> list[int]:
    rules = TICKET_GAME_RULES[lotto_type]
    if lotto_type == "FL":
        return sorted(numbers[:6])
    main = sorted(numbers[:5])
    bonus = numbers[5]
    return main + [bonus]


def validate_ticket_numbers(lotto_type: str, numbers: list[int]) -> tuple[bool, str]:
    rules = TICKET_GAME_RULES[lotto_type]
    required = 6
    if len(numbers) != required:
        return False, f"Exactly {required} numbers are required."

    if lotto_type == "FL":
        if len(set(numbers)) != len(numbers):
            return False, "Ticket numbers must all be different."
        if any(n < 1 or n > rules["main_max"] for n in numbers):
            return False, f"Florida numbers must be between 1 and {rules['main_max']}."
        return True, ""

    main = numbers[:5]
    bonus = numbers[5]
    if len(set(main)) != len(main):
        return False, "Main ticket numbers must all be different."
    if any(n < 1 or n > rules["main_max"] for n in main):
        return False, f"Main numbers must be between 1 and {rules['main_max']}."
    if bonus < 1 or bonus > rules["bonus_max"]:
        return False, f"The 6th number must be between 1 and {rules['bonus_max']}."
    return True, ""


def compare_ticket_to_draw(lotto_type: str, ticket: dict, draw: dict | None) -> dict:
    numbers = _ticket_numbers_from_row(ticket)
    if not draw:
        return {
            "status": "pending",
            "main_matches": 0,
            "bonus_match": False,
            "match_label": "Pending draw",
            "win_amount": None,
            "win_note": "No winning numbers yet.",
            "is_winner": False,
        }

    if lotto_type == "FL":
        ticket_set = set(numbers[:6])
        draw_set = set(_ticket_numbers_from_row(draw)[:6])
        main_matches = len(ticket_set & draw_set)
        match_label = f"{main_matches} of 6"
        return {
            "status": "drawn",
            "main_matches": main_matches,
            "bonus_match": False,
            "match_label": match_label,
            "win_amount": None,
            "win_note": "Florida Lotto payout not derived locally.",
            "is_winner": main_matches >= 3,
        }

    ticket_main = set(numbers[:5])
    draw_numbers = _ticket_numbers_from_row(draw)
    draw_main = set(draw_numbers[:5])
    bonus_match = numbers[5] == draw_numbers[5]
    main_matches = len(ticket_main & draw_main)
    key = (main_matches, bonus_match)
    win_amount = FIXED_PRIZE_TABLES.get(lotto_type, {}).get(key)

    if lotto_type in {"CA", "MM"}:
        note = "Match tier available; payout varies or needs multiplier."
        if bonus_match:
            label = f"{main_matches} + bonus"
        else:
            label = f"{main_matches}"
        return {
            "status": "drawn",
            "main_matches": main_matches,
            "bonus_match": bonus_match,
            "match_label": label,
            "win_amount": win_amount,
            "win_note": note,
            "is_winner": (main_matches >= 3) or bonus_match,
        }

    if bonus_match:
        label = f"{main_matches} + bonus"
    else:
        label = f"{main_matches}"
    return {
        "status": "drawn",
        "main_matches": main_matches,
        "bonus_match": bonus_match,
        "match_label": label,
        "win_amount": win_amount,
        "win_note": "Base prize table only. Add-ons are not included." if win_amount is not None else "No prize.",
        "is_winner": win_amount is not None and win_amount > 0,
    }


# ---------------------------------------------------------------------------
# Login / Logout / Home
# ---------------------------------------------------------------------------

@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        if (request.form.get("username") == AUTH_USER and
                request.form.get("password") == AUTH_PASS):
            session["logged_in"] = True
            next_url = request.args.get("next") or url_for("home")
            return redirect(next_url)
        error = "Invalid username or password."
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/home")
@login_required
def home():
    return render_template("home.html")


@app.route("/tickets")
@login_required
def tickets_page():
    trigger_ticket_cleanup_async()
    lotto_type = request.args.get("lotto", "MM")
    if lotto_type not in LOTTO_LABELS:
        lotto_type = "MM"

    earliest_str, latest_str = db.get_date_bounds(lotto_type)
    latest = date.fromisoformat(latest_str) if latest_str else date.today()
    selected = parse_date_arg(request.args.get("draw_date"), default_ticket_draw_date(lotto_type, latest))

    response = make_response(render_template(
        "tickets.html",
        lotto_type=lotto_type,
        draw_date=selected.isoformat(),
        lotto_types=list(LOTTO_LABELS.keys()),
        lotto_labels=LOTTO_LABELS,
        game_rules=TICKET_GAME_RULES,
        app_build=APP_BUILD,
    ))
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/api/build")
def api_build():
    return jsonify({"build": APP_BUILD})


@app.route("/api/tickets")
@login_required
def api_tickets_get():
    trigger_ticket_cleanup_async()
    lotto_type = request.args.get("lotto", "MM")
    draw_date = request.args.get("draw_date")
    if not draw_date:
        return jsonify({"error": "draw_date required"}), 400

    tickets = db_ticket_sim.get_tickets(lotto_type, draw_date)
    actual_draw = db.get_draw_by_date(lotto_type, draw_date)
    compared = []
    total_won = 0.0
    unknown_wins = 0

    for ticket in tickets:
        result = compare_ticket_to_draw(lotto_type, ticket, actual_draw)
        ticket["comparison"] = result
        compared.append(ticket)
        if not ticket.get("Purchased"):
            continue
        if result["win_amount"] is not None:
            total_won += float(result["win_amount"])
        elif result["is_winner"]:
            unknown_wins += 1

    return jsonify({
        "tickets": compared,
        "actual_draw": actual_draw,
        "total_spent": db_ticket_sim.get_total_spent(lotto_type, draw_date),
        "total_won": total_won,
        "unknown_wins": unknown_wins,
    })


@app.route("/api/tickets", methods=["POST"])
@login_required
def api_tickets_add():
    trigger_ticket_cleanup_async()
    data = request.get_json(silent=True) or {}
    lotto_type = (data.get("lotto") or "MM").strip()
    draw_date = (data.get("draw_date") or "").strip()
    numbers = data.get("numbers") or []
    price = float(data.get("price") or 0)
    purchased = _parse_purchased_flag(data.get("purchased", True))

    if lotto_type not in LOTTO_LABELS:
        return jsonify({"error": "Invalid lotto type"}), 400
    if not draw_date:
        return jsonify({"error": "draw_date required"}), 400
    try:
        date.fromisoformat(draw_date)
    except ValueError:
        return jsonify({"error": "Invalid draw date"}), 400

    try:
        parsed = [int(n) for n in numbers]
    except (TypeError, ValueError):
        return jsonify({"error": "Numbers must be integers"}), 400

    parsed = normalize_ticket_numbers(lotto_type, parsed)
    ok, msg = validate_ticket_numbers(lotto_type, parsed)
    if not ok:
        return jsonify({"error": msg}), 400

    ticket_id = db_ticket_sim.add_ticket(lotto_type, draw_date, price, parsed, purchased=purchased)
    if ticket_id is None:
        return jsonify({"error": "This exact ticket is already saved for that lotto and draw date."}), 409
    return jsonify({"id": ticket_id}), 201


@app.route("/api/tickets/permutations", methods=["POST"])
@login_required
def api_tickets_permutations():
    try:
        trigger_ticket_cleanup_async()
        data = request.get_json(silent=True) or {}
        lotto_type = (data.get("lotto") or "MM").strip()
        draw_date = (data.get("draw_date") or "").strip()
        buckets = data.get("buckets") or []
        price = float(data.get("price") or 0)
        purchased = _parse_purchased_flag(data.get("purchased", True))

        if lotto_type not in LOTTO_LABELS:
            return jsonify({"error": "Invalid lotto type"}), 400
        if not draw_date:
            return jsonify({"error": "draw_date required"}), 400
        if len(buckets) != 6:
            return jsonify({"error": "Six position buckets are required"}), 400

        parsed_buckets = []
        try:
            for bucket in buckets:
                values = [int(v) for v in bucket]
                if not values:
                    return jsonify({"error": "Each position needs at least one number"}), 400
                parsed_buckets.append(values)
        except (TypeError, ValueError):
            return jsonify({"error": "Permutation values must be integers"}), 400

        saved = 0
        invalid = 0
        duplicates = 0
        seen_batch = set()
        for combo in product(*parsed_buckets):
            ticket = normalize_ticket_numbers(lotto_type, list(combo))
            ok, _ = validate_ticket_numbers(lotto_type, ticket)
            if not ok:
                invalid += 1
                continue
            key = tuple(ticket)
            if key in seen_batch:
                duplicates += 1
                continue
            seen_batch.add(key)
            ticket_id = db_ticket_sim.add_ticket(lotto_type, draw_date, price, ticket, purchased=purchased)
            if ticket_id is None:
                duplicates += 1
                continue
            saved += 1

        return jsonify({"saved": saved, "invalid": invalid, "duplicates": duplicates}), 201
    except Exception as exc:
        logger.exception("Could not generate ticket permutations")
        return jsonify({"error": f"Could not generate tickets on the server: {exc}"}), 500


@app.route("/api/manual_draw", methods=["POST"])
@login_required
def api_manual_draw():
    data = request.get_json(silent=True) or {}
    lotto_type = (data.get("lotto") or "MM").strip().upper()
    draw_date = (data.get("draw_date") or "").strip()
    numbers = data.get("numbers") or []
    overwrite = _parse_purchased_flag(data.get("overwrite", False))

    if lotto_type not in LOTTO_LABELS:
        return jsonify({"error": "Invalid lotto type"}), 400
    if not draw_date:
        return jsonify({"error": "draw_date required"}), 400
    try:
        date.fromisoformat(draw_date)
    except ValueError:
        return jsonify({"error": "Invalid draw date"}), 400
    if not isinstance(numbers, list) or len(numbers) != 6:
        return jsonify({"error": "Exactly 6 winning numbers are required"}), 400

    try:
        parsed = [int(n) for n in numbers]
    except (TypeError, ValueError):
        return jsonify({"error": "Numbers must be integers"}), 400

    parsed = normalize_ticket_numbers(lotto_type, parsed)
    ok, msg = validate_ticket_numbers(lotto_type, parsed)
    if not ok:
        return jsonify({"error": msg}), 400

    existing = db.get_draw_by_date(lotto_type, draw_date)
    if existing and not overwrite:
        return jsonify({
            "error": "Winning numbers for that lotto type and draw date already exist.",
            "existing": existing,
        }), 409

    if existing:
        saved = db.update_draw(
            lotto_type,
            draw_date,
            parsed[0],
            parsed[1],
            parsed[2],
            parsed[3],
            parsed[4],
            parsed[5],
        )
    else:
        saved = db.insert_draw(
            lotto_type,
            draw_date,
            parsed[0],
            parsed[1],
            parsed[2],
            parsed[3],
            parsed[4],
            parsed[5],
        )
    if not saved:
        return jsonify({"error": "Could not save the winning numbers."}), 500

    _refresh_forecasts_for_lotto(lotto_type)

    return jsonify({
        "saved": True,
        "updated": bool(existing),
        "lotto": lotto_type,
        "draw_date": draw_date,
        "numbers": parsed,
    }), 201


@app.route("/api/tickets/<int:ticket_id>", methods=["DELETE", "POST"])
@login_required
def api_tickets_delete(ticket_id):
    deleted = db_ticket_sim.delete_ticket(ticket_id)
    if not deleted:
        # Treat stale rows as effectively deleted so the UI can refresh cleanly.
        return jsonify({"deleted": ticket_id, "missing": True})
    return jsonify({"deleted": ticket_id})


@app.route("/api/tickets/<int:ticket_id>/status", methods=["POST"])
@login_required
def api_tickets_update_status(ticket_id):
    data = request.get_json(silent=True) or {}
    purchased = _parse_purchased_flag(data.get("purchased", False))
    updated = db_ticket_sim.update_ticket_status(ticket_id, purchased)
    if not updated:
        return jsonify({"error": "Ticket not found"}), 404
    return jsonify({"id": ticket_id, "purchased": purchased})


# ---------------------------------------------------------------------------
# Main viewer page
# ---------------------------------------------------------------------------

@app.route("/")
@login_required
def index():
    lotto_type  = request.args.get("lotto", "CA")
    days        = int(request.args.get("days", DEFAULT_DAYS))
    if days not in WINDOW_DAYS:
        days = DEFAULT_DAYS

    ensure_lotto_draws_current(lotto_type)

    earliest_str, latest_str = db.get_date_bounds(lotto_type)
    if not latest_str:
        latest_str = date.today().isoformat()
    if not earliest_str:
        earliest_str = latest_str

    earliest = date.fromisoformat(earliest_str)
    latest   = date.fromisoformat(latest_str)

    # anchor = dtCutOffNow, defaults to latest draw date
    anchor_str = request.args.get("anchor", latest_str)
    try:
        anchor = date.fromisoformat(anchor_str)
    except ValueError:
        anchor = latest

    # Clamp anchor to [earliest + eff_days, latest]
    eff = effective_days(lotto_type, days)
    anchor = max(earliest + timedelta(days=eff), min(latest, anchor))

    past = cutoff_past(anchor, eff)

    index_lo, index_hi = db.get_index_range(lotto_type)
    lotto_types = db.get_lotto_types()

    return render_template(
        "viewer.html",
        lotto_type    = lotto_type,
        lotto_types   = lotto_types,
        lotto_labels  = LOTTO_LABELS,
        days          = days,
        window_days   = WINDOW_DAYS,
        anchor        = anchor.isoformat(),
        cutoff_past   = past.isoformat(),
        earliest      = earliest_str,
        latest        = latest_str,
        index_lo      = index_lo,
        index_hi      = index_hi,
        eff_days      = eff,
    )


# ---------------------------------------------------------------------------
# API: draw data for a date window
# ---------------------------------------------------------------------------

@app.route("/api/draws")
@login_required
def api_draws():
    lotto_type   = request.args.get("lotto", "CA")
    cutoff_now   = request.args.get("cutoff_now")
    cutoff_past_ = request.args.get("cutoff_past")
    if not cutoff_now or not cutoff_past_:
        return jsonify({"error": "cutoff_now and cutoff_past required"}), 400
    ensure_lotto_draws_current(lotto_type)
    draws = db.get_draws_in_window(lotto_type, cutoff_past_, cutoff_now)
    return jsonify(draws)


# ---------------------------------------------------------------------------
# API: scrollbar → draw index → date (dtCutOffNow candidate)
# ---------------------------------------------------------------------------

@app.route("/api/index_to_date")
@login_required
def api_index_to_date():
    lotto_type = request.args.get("lotto", "CA")
    try:
        draw_index = int(request.args.get("index"))
    except (TypeError, ValueError):
        return jsonify({"error": "index required"}), 400
    d = db.get_date_for_index(lotto_type, draw_index)
    return jsonify({"index": draw_index, "date": d})


# ---------------------------------------------------------------------------
# API: single draw by index (plot click inspection)
# ---------------------------------------------------------------------------

@app.route("/api/draw")
@login_required
def api_draw():
    lotto_type = request.args.get("lotto", "CA")
    try:
        draw_index = int(request.args.get("index"))
    except (TypeError, ValueError):
        return jsonify({"error": "index required"}), 400
    draw = db.get_draw_by_index(lotto_type, draw_index)
    if draw is None:
        return jsonify({"error": "not found"}), 404
    return jsonify(draw)


# ---------------------------------------------------------------------------
# API: navigation — returns new anchor date
# Implements Mode A only (period mode).
# ---------------------------------------------------------------------------

@app.route("/api/nav")
@login_required
def api_nav():
    """
    Compute new anchor (dtCutOffNow) for LEFT / RIGHT / START / END.

    Mode A — Period mode:
      END:   anchor = latest_draw_date
      START: anchor = earliest_draw_date + eff_days
      RIGHT: anchor = old_anchor + eff_days, clamped to latest
      LEFT:  anchor = old_anchor - eff_days  (new end = old start)
    """
    lotto_type  = request.args.get("lotto", "CA")
    direction   = request.args.get("dir")          # left|right|start|end
    days        = int(request.args.get("days", DEFAULT_DAYS))
    anchor_str  = request.args.get("anchor", "")

    earliest_str, latest_str = db.get_date_bounds(lotto_type)
    if not latest_str:
        return jsonify({"error": "no data"}), 400

    earliest = date.fromisoformat(earliest_str)
    latest   = date.fromisoformat(latest_str)
    eff      = effective_days(lotto_type, days)

    try:
        anchor = date.fromisoformat(anchor_str)
    except ValueError:
        anchor = latest

    if direction == "end":
        new_anchor = latest
    elif direction == "start":
        new_anchor = earliest + timedelta(days=eff)
    elif direction == "right":
        new_anchor = anchor + timedelta(days=eff)
        new_anchor = min(new_anchor, latest)
    elif direction == "left":
        new_anchor = anchor - timedelta(days=eff)
        # Clamp so we don't go before earliest + eff_days
        new_anchor = max(new_anchor, earliest + timedelta(days=eff))
    else:
        return jsonify({"error": f"unknown direction: {direction}"}), 400

    new_past = cutoff_past(new_anchor, eff)
    return jsonify({
        "anchor":      new_anchor.isoformat(),
        "cutoff_past": new_past.isoformat(),
        "eff_days":    eff,
    })


# ---------------------------------------------------------------------------
# Stage 2 constants
# ---------------------------------------------------------------------------

FORECAST_MODEL = "WF_v4_baseline"


# ---------------------------------------------------------------------------
# API: forecast bands for the current viewer window
#
# Returns one row per (DrawDate, SetNumber) within [cutoff_past, cutoff_now].
# Each row: { DrawDate, SetNumber, SafeLow, SafeHigh, HotLow, HotHigh }
# HotLow / HotHigh may be null.
# ---------------------------------------------------------------------------

@app.route("/api/forecast")
@login_required
def api_forecast():
    lotto_type   = request.args.get("lotto", "CA")
    cutoff_now   = request.args.get("cutoff_now")
    cutoff_past_ = request.args.get("cutoff_past")
    if not cutoff_now or not cutoff_past_:
        return jsonify({"error": "cutoff_now and cutoff_past required"}), 400
    bands = db_forecast.get_forecast_bands(
        lotto_type, cutoff_past_, cutoff_now, FORECAST_MODEL
    )
    return jsonify(bands)


# ---------------------------------------------------------------------------
# API: forecast chart data — bands + actual drawn values
#
# GET /api/forecast_chart?lotto=CA&cutoff_now=2026-02-14&cutoff_past=2025-03-01
#
# Returns one row per (DrawDate, SetNumber) ordered by DrawDate ASC, SetNumber ASC.
# Each row: { DrawIndex, DrawDate, SetNumber,
#             ActualValue, SafeLow, SafeHigh, HotLow, HotHigh }
#
# 5 series per set (maps directly to visual elements):
#   ActualValue -> black solid line    (actual drawn number)
#   SafeHigh    -> teal dashed upper   (SAFE band top)
#   SafeLow     -> teal dashed lower   (SAFE band bottom)
#   HotHigh     -> red dashed upper    (HOT band top)
#   HotLow      -> red dashed lower    (HOT band bottom)
# ---------------------------------------------------------------------------

@app.route("/api/forecast_chart")
@login_required
def api_forecast_chart():
    lotto_type   = request.args.get("lotto", "CA")
    cutoff_now   = request.args.get("cutoff_now")
    cutoff_past_ = request.args.get("cutoff_past")
    if not cutoff_now or not cutoff_past_:
        return jsonify({"error": "cutoff_now and cutoff_past required"}), 400
    rows = db_forecast.get_forecast_chart_data(
        lotto_type, cutoff_past_, cutoff_now, FORECAST_MODEL
    )
    return jsonify(rows)


# ---------------------------------------------------------------------------
# _backfill_missing  (startup helper, not a route)
#
# For each lotto type, if ForecastPredictions has no rows yet,
# run a full BackfillPredictions pass.  Already-populated types are skipped
# instantly via get_last_forecast_date.  New draw dates added by the scraper
# after startup are handled by the nightly incremental pass in the scraper
# background thread (future extension point).
# ---------------------------------------------------------------------------

def _backfill_missing() -> None:
    from forecast import backfill_predictions
    for lt in ["CA", "FL", "MM", "PB", "PD"]:
        last = db_forecast.get_last_forecast_date(lt, FORECAST_MODEL)
        if last is None:
            logger.info("Backfilling %s forecast (first run)...", lt)
            dates = db_forecast.get_draw_dates(lt)
            n = backfill_predictions(lt, dates, FORECAST_MODEL, _dal=db_forecast)
            logger.info("Backfill %s complete: %d dates", lt, n)
        else:
            # Incremental: pick up any draw dates after the last forecasted date
            new_dates = db_forecast.get_draw_dates_after(lt, last)
            # Exclude last itself (already done); only truly new dates
            new_dates = [d for d in new_dates if d > last]
            if new_dates:
                logger.info("Incremental backfill %s: %d new date(s)", lt, len(new_dates))
                from forecast import backfill_predictions
                backfill_predictions(lt, new_dates, FORECAST_MODEL, _dal=db_forecast)
            else:
                logger.info("%s forecast up to date (last: %s)", lt, last)


# ---------------------------------------------------------------------------
# API: ranked candidate combinations for a specific draw date
#
# GET /api/selections?lotto=CA&draw_date=2026-02-14
#
# Returns up to TOP_N combinations ordered by Score DESC.
# Each row: { CombinationId, LottoType, DrawDate,
#             Nbr1..Nbr6, Score, SelectionReason }
#
# Computes on the fly from Stage 2 bands — no pre-population required.
# ---------------------------------------------------------------------------

@app.route("/api/selections")
@login_required
def api_selections():
    lotto_type = request.args.get("lotto", "CA")
    draw_date  = request.args.get("draw_date")
    if not draw_date:
        return jsonify({"error": "draw_date required"}), 400
    combos = selection.select_for_draw(
        lotto_type, draw_date, FORECAST_MODEL, _dal=db_forecast
    )
    return jsonify([c.as_dict() for c in combos])



# ---------------------------------------------------------------------------
# Gap-Pattern Matching page
# ---------------------------------------------------------------------------

@app.route("/gaps")
@login_required
def gaps_page():
    lotto_type = request.args.get("lotto", "CA")
    mode = request.args.get("mode", "directions")
    if mode not in {"directions", "jumps"}:
        mode = "directions"
    return render_template(
        "gaps.html",
        lotto_type=lotto_type,
        gap_mode=mode,
        lotto_types=list(LOTTO_LABELS.keys()),
        lotto_labels=LOTTO_LABELS,
    )


# ---------------------------------------------------------------------------
# API: gap-pattern matches
#
# GET /api/gaps?lotto=CA
# Returns up to 3 match records (see gap_engine.find_matches).
# ---------------------------------------------------------------------------

@app.route("/api/gaps")
@login_required
def api_gaps():
    lotto_type = request.args.get("lotto", "CA")
    mode = request.args.get("mode", "directions")
    draws = db.get_all_draws(lotto_type)
    if not draws:
        return jsonify([])
    if mode == "jumps":
        matches = gap_engine.find_jump_matches(draws)
    else:
        matches = gap_engine.find_matches(draws)
    return jsonify(matches)


# ---------------------------------------------------------------------------
# YouTube Links page
# ---------------------------------------------------------------------------

CATEGORIES = [
    "music",
    "cooking",
    "baking",
    "fashion",
    "med. remedies",
    "med. medicine",
    "dancing",
    "singers",
    "music classical",
    "music georgian",
    "music caucasian",
    "musical writing skills",
    "literature poems",
    "literature novels",
    "literature writing skills",
]


@app.route("/links")
@login_required
def links_page():
    return render_template("links.html", categories=CATEGORIES)


# ---------------------------------------------------------------------------
# API: get all links
# GET /api/links  →  { music: [...], cooking: [...], baking: [...] }
# ---------------------------------------------------------------------------

@app.route("/api/links")
@login_required
def api_links_get():
    all_links = db_links.get_all_links()
    grouped = {cat: [] for cat in CATEGORIES}
    for link in all_links:
        cat = link["Category"]
        if cat in grouped:
            grouped[cat].append(link)
    return jsonify(grouped)


# ---------------------------------------------------------------------------
# API: add link
# POST /api/links   body: { category, url }
# ---------------------------------------------------------------------------

@app.route("/api/links", methods=["POST"])
@login_required
def api_links_add():
    data = request.get_json(silent=True) or {}
    category = (data.get("category") or "").strip().lower()
    url      = (data.get("url") or "").strip()

    if category not in CATEGORIES:
        return jsonify({"error": f"Category must be one of: {', '.join(CATEGORIES)}"}), 400
    if not url:
        return jsonify({"error": "URL is required"}), 400

    # URL validation
    try:
        links_fetcher.validate_url(url)
    except links_fetcher.FetchError as e:
        return jsonify({"error": str(e)}), 400

    # Cap check
    if db_links.count_links() >= db_links.MAX_LINKS:
        return jsonify({"error": f"Maximum of {db_links.MAX_LINKS} links reached"}), 400

    # Fetch title
    try:
        title = links_fetcher.fetch_title(url)
    except links_fetcher.FetchError:
        return jsonify({"error": "Could not read video title"}), 422

    new_id = db_links.add_link(category, title, url)
    link = db_links.get_link(new_id)
    return jsonify(link), 201


# ---------------------------------------------------------------------------
# API: fetch title only (used by edit form on URL change)
# POST /api/links/fetch_title   body: { url }
# ---------------------------------------------------------------------------

@app.route("/api/links/fetch_title", methods=["POST"])
@login_required
def api_links_fetch_title():
    data = request.get_json(silent=True) or {}
    url  = (data.get("url") or "").strip()
    if not url:
        return jsonify({"error": "URL is required"}), 400
    try:
        links_fetcher.validate_url(url)
        title = links_fetcher.fetch_title(url)
        return jsonify({"title": title})
    except links_fetcher.FetchError:
        return jsonify({"error": "Could not read video title"}), 422


# ---------------------------------------------------------------------------
# API: update link
# PUT /api/links/<id>   body: { category, url }
# ---------------------------------------------------------------------------

@app.route("/api/links/<int:link_id>", methods=["PUT"])
@login_required
def api_links_update(link_id):
    data = request.get_json(silent=True) or {}
    category = (data.get("category") or "").strip().lower()
    url      = (data.get("url") or "").strip()

    if category not in CATEGORIES:
        return jsonify({"error": f"Category must be one of: {', '.join(CATEGORIES)}"}), 400
    if not url:
        return jsonify({"error": "URL is required"}), 400

    existing = db_links.get_link(link_id)
    if not existing:
        return jsonify({"error": "Link not found"}), 404

    try:
        links_fetcher.validate_url(url)
    except links_fetcher.FetchError as e:
        return jsonify({"error": str(e)}), 400

    # Re-fetch title if URL changed
    if url != existing["Url"]:
        try:
            title = links_fetcher.fetch_title(url)
        except links_fetcher.FetchError:
            return jsonify({"error": "Could not read video title"}), 422
    else:
        title = existing["Title"]

    db_links.update_link(link_id, category, title, url)
    return jsonify(db_links.get_link(link_id))


# ---------------------------------------------------------------------------
# API: delete link
# DELETE /api/links/<id>
# ---------------------------------------------------------------------------

@app.route("/api/links/<int:link_id>", methods=["DELETE"])
@login_required
def api_links_delete(link_id):
    if not db_links.get_link(link_id):
        return jsonify({"error": "Link not found"}), 404
    db_links.delete_link(link_id)
    return jsonify({"deleted": link_id})


# ---------------------------------------------------------------------------
# _populate_selections  (startup helper, not a route)
#
# Persists Stage 3 combinations for the most recent draw date of each
# lotto type.  Idempotent — INSERT OR IGNORE means re-runs are safe.
# Only the latest draw is populated at startup; historical dates are
# computed on-the-fly by /api/selections when requested.
# ---------------------------------------------------------------------------

def _populate_selections() -> None:
    for lt in ["CA", "FL", "MM", "PB", "PD"]:
        last_date = db_forecast.get_last_forecast_date(lt, FORECAST_MODEL)
        if not last_date:
            logger.info("No forecast bands for %s — skipping Stage 3", lt)
            continue
        if db_selection.combinations_exist(lt, last_date, FORECAST_MODEL):
            logger.info("%s Stage 3 up to date (last: %s)", lt, last_date)
            continue
        combos = selection.select_for_draw(
            lt, last_date, FORECAST_MODEL, _dal=db_forecast
        )
        if combos:
            db_selection.persist_combinations_versioned(combos, FORECAST_MODEL)
            logger.info("%s Stage 3: persisted %d combos for %s", lt, len(combos), last_date)
        else:
            logger.warning("%s Stage 3: no combos generated for %s", lt, last_date)


def initialize_runtime() -> None:
    global _runtime_initialized
    with _runtime_init_lock:
        if _runtime_initialized:
            return

        logger.info("Using lotto DB at %s", db.DB_PATH)

        is_render_runtime = _is_render_runtime()
        xlsx = Path(__file__).resolve().parent.parent / "data" / "Lotto.xlsx"
        db_exists = Path(db.DB_PATH).exists()
        if not db_exists:
            logger.info("Initialising database...")
        db.init_db()
        if xlsx.exists() and (not db_exists or not is_render_runtime):
            summary = db.ingest_xlsx(str(xlsx))
            logger.info("Workbook sync complete: %s", summary)
        elif xlsx.exists():
            logger.info("Skipping workbook sync on Render because persistent DB already exists.")
        else:
            logger.warning("Seed workbook not found at %s", xlsx)

        db_forecast.init_forecast_schema()
        # On Render, keep forecasts incrementally refreshed so newly scraped
        # draw dates appear without requiring a manual rebuild of the DB.
        run_forecast_bootstrap = _env_flag("LOTTO_BOOTSTRAP_FORECASTS", True)
        if run_forecast_bootstrap:
            _backfill_missing()
        else:
            logger.info("Skipping forecast bootstrap during startup.")

        db_selection.init_selection_schema()
        run_selection_bootstrap = _env_flag("LOTTO_BOOTSTRAP_SELECTIONS", not is_render_runtime)
        if run_selection_bootstrap:
            _populate_selections()
        else:
            logger.info("Skipping selection bootstrap during startup.")

        db_links.init_links_schema()
        db_ticket_sim.init_ticket_schema()
        trigger_ticket_cleanup_async(force=True)

        # Run the scraper on Render too so the persistent database does not
        # get stuck on an old latest draw date.
        if _env_flag("LOTTO_BACKGROUND_SCRAPER", True):
            scraper.start_background_scraper()

        _runtime_initialized = True


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

initialize_runtime()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
