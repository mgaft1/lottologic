"""
app.py  --  Lotto Viewer Phase 1
Window slicing by calendar days. Mode A navigation only.
PB/PD: effective_days = round(days * 0.6).
Background scraper fills missing draws; never blocks rendering.
"""

import logging
import os
import re
import sqlite3
import sys
import threading
import requests
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

PROFILE_BOUNDARY_DATES = {
    "CA": date(2000, 6, 4),
    "FL": date(2020, 1, 1),
    "MM": date(2013, 10, 12),
    "PB": date(2015, 7, 29),
    "PD": date(2015, 7, 29),
}

SELECTED_LOTTO_SESSION_KEY = "selected_lotto"


def _valid_lotto_or_default(value: str | None, default: str = "CA") -> str:
    lotto_type = (value or "").strip().upper()
    if lotto_type in LOTTO_LABELS:
        return lotto_type
    return default if default in LOTTO_LABELS else "CA"


def _selected_lotto(default: str = "CA") -> str:
    return _valid_lotto_or_default(session.get(SELECTED_LOTTO_SESSION_KEY), default)


def _resolve_lotto_arg(default: str = "CA") -> str:
    lotto_type = _valid_lotto_or_default(request.args.get("lotto"), _selected_lotto(default))
    session[SELECTED_LOTTO_SESSION_KEY] = lotto_type
    return lotto_type


def _resolve_lotto_payload(data: dict, default: str = "MM") -> str:
    lotto_type = _valid_lotto_or_default(data.get("lotto"), _selected_lotto(default))
    session[SELECTED_LOTTO_SESSION_KEY] = lotto_type
    return lotto_type

TICKET_GAME_RULES = {
    "CA": {"main_count": 5, "main_max": 47, "bonus_max": 27, "base_price": 1.0},
    "FL": {"main_count": 6, "main_max": 53, "bonus_max": None, "base_price": 2.0},
    "MM": {"main_count": 5, "main_max": 70, "bonus_max": 24, "base_price": 5.0},
    "PB": {"main_count": 5, "main_max": 69, "bonus_max": 26, "base_price": 2.0},
    "PD": {"main_count": 5, "main_max": 69, "bonus_max": 26, "base_price": 1.0},
}
MAX_TICKET_PERMUTATIONS = int(os.environ.get("LOTTO_MAX_TICKET_PERMUTATIONS", "5000"))

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


def trigger_forecast_refresh_async(lotto_type: str) -> None:
    def _forecast_job() -> None:
        try:
            _refresh_forecasts_for_lotto(lotto_type)
        except Exception as exc:
            logger.warning("%s forecast refresh after manual draw failed: %s", lotto_type, exc)

    threading.Thread(
        target=_forecast_job,
        name=f"manual-draw-forecast-{lotto_type.lower()}",
        daemon=True,
    ).start()


def ensure_lotto_draws_current(lotto_type: str) -> None:
    """
    Viewer stale-data guard. If the DB's latest draw date is behind the most
    recent completed scheduled draw for this lotto type, attempt a targeted
    refresh before serving data.
    """
    if _is_render_runtime() and not _env_flag("LOTTO_RENDER_REQUEST_REFRESH", False):
        return

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


def _project_future_draw(lotto_type: str, match: dict, current_draw: dict) -> dict:
    rules = TICKET_GAME_RULES[lotto_type]
    projected = []
    for set_num in range(1, 7):
        anchor = match["anchor_draw"].get(f"Nbr{set_num}")
        next_value = match["next_draw"].get(f"Nbr{set_num}")
        current = current_draw.get(f"Nbr{set_num}")
        if anchor is None or next_value is None or current is None:
            projected.append(None)
            continue
        max_value = rules["main_max"] if lotto_type == "FL" or set_num <= 5 else rules["bonus_max"]
        value = int(current) + (int(next_value) - int(anchor))
        projected.append(max(1, min(int(max_value), value)))

    if lotto_type != "FL":
        main = sorted(v for v in projected[:5] if v is not None)
        projected = main + [projected[5]]
    else:
        projected = sorted(v for v in projected if v is not None)

    return {f"set{idx + 1}": value for idx, value in enumerate(projected)}


def _first_projected_match(lotto_type: str, mode: str, draws: list[dict]) -> dict | None:
    if not draws:
        return None
    matches = gap_engine.find_jump_matches(draws) if mode == "jumps" else gap_engine.find_matches(draws)
    if not matches:
        return None
    match = matches[0]
    return {
        "match_count": len(matches),
        "anchor_index": match["anchor_index"],
        "anchor_date": match["anchor_date"],
        "numbers": _project_future_draw(lotto_type, match, draws[-1]),
    }


def _overdue_numbers(lotto_type: str, draws: list[dict]) -> list[dict]:
    if not draws:
        return []
    rules = TICKET_GAME_RULES[lotto_type]
    max_number = rules["main_max"]
    if rules.get("bonus_max"):
        max_number = max(max_number, rules["bonus_max"])

    rows = []
    latest_first = list(reversed(draws))
    for number in range(1, int(max_number) + 1):
        draws_since = len(draws)
        last_date = None
        for offset, draw in enumerate(latest_first):
            values = [draw.get(f"Nbr{set_num}") for set_num in range(1, 7)]
            if number in values:
                draws_since = offset
                last_date = draw.get("DrawDate")
                break
        rows.append({
            "number": number,
            "draws_since": draws_since,
            "last_date": last_date,
        })

    rows.sort(key=lambda row: (-row["draws_since"], row["number"]))
    return rows


def _profile_draws(lotto_type: str, draws: list[dict]) -> list[dict]:
    boundary = PROFILE_BOUNDARY_DATES.get(lotto_type)
    if not boundary:
        return draws
    filtered = []
    for draw in draws:
        draw_date = draw.get("DrawDate")
        if not draw_date:
            continue
        try:
            if date.fromisoformat(draw_date) >= boundary:
                filtered.append(draw)
        except ValueError:
            continue
    return filtered


def _weighted_overdue_numbers(lotto_type: str, draws: list[dict]) -> list[dict]:
    profile_draws = _profile_draws(lotto_type, draws)
    if not profile_draws:
        return []

    rules = TICKET_GAME_RULES[lotto_type]
    max_number = rules["main_max"]
    if rules.get("bonus_max"):
        max_number = max(max_number, rules["bonus_max"])

    weights = [0.91 + (idx * 0.01) for idx in range(10)]
    latest_draw_index = len(profile_draws) - 1
    rows = []

    for number in range(1, int(max_number) + 1):
        hit_indexes = []
        hit_dates = []
        for idx, draw in enumerate(profile_draws):
            values = [draw.get(f"Nbr{set_num}") for set_num in range(1, 7)]
            if number in values:
                hit_indexes.append(idx)
                hit_dates.append(draw.get("DrawDate"))

        if not hit_indexes:
            continue

        current_gap = latest_draw_index - hit_indexes[-1]
        completed_gaps = [
            hit_indexes[idx] - hit_indexes[idx - 1]
            for idx in range(1, len(hit_indexes))
        ]
        if completed_gaps:
            historical_average_gap = sum(completed_gaps) / len(completed_gaps)
        else:
            historical_average_gap = float(max(current_gap, 1))

        recent_gaps = completed_gaps[-9:] + [current_gap]
        applied_weights = weights[-len(recent_gaps):]
        weighted_recent_average = sum(
            gap * weight for gap, weight in zip(recent_gaps, applied_weights)
        ) / sum(applied_weights)
        weighted_ratio = weighted_recent_average / historical_average_gap if historical_average_gap else 0.0

        rows.append({
            "number": number,
            "weighted_ratio": round(weighted_ratio, 4),
            "current_gap": current_gap,
            "historical_average_gap": round(historical_average_gap, 2),
            "weighted_recent_average": round(weighted_recent_average, 2),
            "recent_gap_count": len(recent_gaps),
            "last_date": hit_dates[-1],
            "hit_count": len(hit_indexes),
        })

    rows.sort(key=lambda row: (-row["weighted_ratio"], -row["current_gap"], row["number"]))
    return rows


def _set_overdue_numbers(lotto_type: str, draws: list[dict], set_number: int) -> list[dict]:
    if not draws or set_number < 1 or set_number > 6:
        return []

    rules = TICKET_GAME_RULES[lotto_type]
    max_number = rules["main_max"] if lotto_type == "FL" or set_number <= 5 else rules["bonus_max"]
    if not max_number:
        return []

    set_key = f"Nbr{set_number}"
    latest_draw_index = len(draws) - 1
    rows = []

    for number in range(1, int(max_number) + 1):
        hit_indexes = [
            idx for idx, draw in enumerate(draws)
            if draw.get(set_key) == number
        ]
        if not hit_indexes:
            continue

        last_idx = hit_indexes[-1]
        current_gap = latest_draw_index - last_idx
        last_date = draws[last_idx].get("DrawDate")

        if len(hit_indexes) > 1:
            gaps = [
                hit_indexes[idx] - hit_indexes[idx - 1]
                for idx in range(1, len(hit_indexes))
            ]
            average_gap = sum(gaps) / len(gaps)
        else:
            average_gap = len(draws)

        if current_gap >= average_gap:
            rows.append({
                "set_number": set_number,
                "number": number,
                "current_gap": current_gap,
                "average_gap": round(average_gap, 1),
                "last_date": last_date,
                "hit_count": len(hit_indexes),
            })

    rows.sort(key=lambda row: (-row["current_gap"], row["number"]))
    return rows


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


@app.route("/taro")
@login_required
def taro_page():
    return render_template("taro.html")


@app.route("/numerology")
@login_required
def numerology_page():
    return render_template("numerology.html")


TARO_READER_INSTRUCTIONS = """You are an experienced traditional Tarot reader speaking directly to a client.
Interpret the three-card spread in the context of the client's actual question. Use the cards as a lens for
thoughtful guidance, not as proof of supernatural certainty.

Write a cohesive reading of 350 to 500 words. Start by answering the question directly and honestly. Explain
the symbolism of each card in plain English, including what its spread position and upright or reversed
orientation contribute. Then explain how the cards reinforce, challenge, or modify one another. Make specific
observations tied to the client's question. Avoid generic self-help advice, canned phrases such as "invites
reflection," and abstract filler such as "expressed through emotion." Do not merely list three separate card
definitions. End with one practical takeaway.

For health, legal, financial, or safety-related questions, make clear that the reading is reflective guidance
and not a factual prediction or professional advice. Never promise an outcome."""


def _taro_output_text(response_json):
    parts = []
    for item in response_json.get("output", []):
        if item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if content.get("type") == "output_text" and content.get("text"):
                parts.append(content["text"].strip())
    return "\n\n".join(part for part in parts if part)


TARO_THEMES = (
    ("spiritual connection", {"spiritual", "spiritually", "spirituality", "soul", "souls", "faith", "community", "belong", "belonging", "people", "friends", "friendship"}),
    ("relationship", {"love", "relationship", "partner", "marry", "marriage", "dating", "romance", "romantic"}),
    ("financial", {"rich", "money", "financial", "income", "wealth", "wealthy", "debt", "afford", "investment"}),
    ("career", {"job", "career", "business", "work", "promotion", "profession", "employment"}),
    ("health", {"health", "ill", "sick", "doctor", "healing", "medical"}),
    ("decision", {"choose", "choice", "decide", "decision", "should", "whether"}),
)

TARO_THEME_OPENERS = {
    "spiritual connection": "The cards lean toward connection being possible, but they describe the kind of openness and clarity that will help you recognize the right people.",
    "relationship": "The cards describe the relationship pattern around your question rather than promising a fixed romantic outcome.",
    "financial": "The cards do not promise sudden wealth. They point to the habits, opportunities, and cautions that matter most in your financial question.",
    "career": "The cards show a workable direction in your career question, with attention to both opportunity and the effort it will require.",
    "health": "The cards can help you reflect on your experience, but they cannot diagnose a condition or replace medical advice.",
    "decision": "The cards do not make the decision for you. They clarify what is influencing the choice and what deserves closer attention.",
    "general": "The spread does not give a guaranteed prediction. It describes the pattern around your question and the next useful point of attention.",
}

TARO_MAJOR_MEANINGS = {
    "The Fool": ("The Fool is the traveler at the start of the road. It points to openness, experimentation, and entering unfamiliar territory without needing every answer first.", "The Fool reversed warns against confusing openness with carelessness. A new beginning may still be possible, but it needs better judgment and a slower first step."),
    "The Magician": ("The Magician has tools laid out on the table and knows how to use them. It points to skill, initiative, and turning an idea into something concrete.", "The Magician reversed suggests talent without focus, mixed motives, or effort scattered across too many directions."),
    "The High Priestess": ("The High Priestess sits between the visible and hidden worlds. She points to intuition, private knowledge, and information that has not fully surfaced yet.", "The High Priestess reversed suggests that a quiet signal is being overlooked or that uncertainty is being mistaken for intuition."),
    "The Empress": ("The Empress represents nourishment and growth. She points to conditions in which connection, creativity, or practical abundance can develop naturally.", "The Empress reversed suggests overgiving, neglecting your own needs, or trying to force growth before the conditions are ready."),
    "The Emperor": ("The Emperor represents structure and boundaries. He points to a need for a clear plan, firm standards, and dependable action.", "The Emperor reversed warns that control or rigidity may be getting in the way of a more workable structure."),
    "The Hierophant": ("The Hierophant represents shared beliefs, tradition, and learning in community. It often points to teachers, established groups, or values held in common.", "The Hierophant reversed suggests that an inherited rule or conventional path may not fit; a more personal understanding is needed."),
    "The Lovers": ("The Lovers is about alignment before it is about romance. It points to a meaningful choice, mutual recognition, and acting in a way that matches your values.", "The Lovers reversed suggests mixed signals, competing values, or a connection that cannot deepen until a choice is made honestly."),
    "The Chariot": ("The Chariot moves because opposing forces are held in one direction. It points to momentum, discipline, and actively steering events.", "The Chariot reversed suggests competing priorities or a push for progress that lacks a clear direction."),
    "Strength": ("Strength shows calm influence rather than force. It points to patience, courage, and handling a sensitive situation without overpowering it.", "Strength reversed suggests self-doubt or the temptation to push too hard because confidence is wavering."),
    "The Hermit": ("The Hermit carries a lantern and steps away from noise to see more clearly. It points to solitude, discernment, and a search for what is genuine.", "The Hermit reversed warns that useful solitude may have become isolation or that too much analysis is delaying re-entry into the world."),
    "Wheel of Fortune": ("The Wheel of Fortune marks a change in conditions. It points to timing, cycles, and an opening created by movement that is already underway.", "The Wheel of Fortune reversed suggests a repeated pattern or frustration with timing; the cycle needs to be understood before it can change."),
    "Justice": ("Justice weighs what is true and proportionate. It points to accountability, honest assessment, and choices with clear consequences.", "Justice reversed suggests an imbalance, an incomplete picture, or a need to examine where responsibility has been avoided."),
    "The Hanged Man": ("The Hanged Man pauses willingly to see the situation from another angle. It points to delay with a purpose and the value of releasing an old assumption.", "The Hanged Man reversed suggests stagnation: waiting is no longer producing insight and a different action is needed."),
    "Death": ("Death represents a necessary ending and the space it creates for change. It points to transition, not literal death.", "Death reversed suggests resistance to an ending or an attempt to preserve a pattern that has already run its course."),
    "Temperance": ("Temperance blends different elements carefully. It points to moderation, gradual progress, and finding a combination that can last.", "Temperance reversed warns of extremes, poor pacing, or ingredients that have not yet been brought into balance."),
    "The Devil": ("The Devil reveals what has a grip on you: fear, temptation, habit, or an agreement that no longer feels voluntary. Seeing the attachment clearly is the first step toward choice.", "The Devil reversed suggests that an old attachment is loosening and that more freedom is available than you may have assumed."),
    "The Tower": ("The Tower is the moment an unstable structure can no longer pretend to be secure. It points to disruption, blunt truth, and necessary rebuilding.", "The Tower reversed suggests an avoided change or an internal shake-up that has not yet been addressed openly."),
    "The Star": ("The Star appears after upheaval and represents renewed orientation. It points to hope grounded in healing, honesty, and a clearer sense of what matters.", "The Star reversed suggests discouragement or a loss of confidence that makes a real possibility harder to see."),
    "The Moon": ("The Moon lights a path without making everything clear. It points to ambiguity, imagination, and the need to distinguish intuition from fear.", "The Moon reversed suggests that confusion is beginning to lift, although some assumptions still need to be tested."),
    "The Sun": ("The Sun brings visibility and warmth. It points to confidence, openness, and a situation becoming easier to understand.", "The Sun reversed suggests that a positive development is delayed, muted, or harder to enjoy because expectations are too rigid."),
    "Judgement": ("Judgement is a wake-up call. It points to reviewing the past honestly and answering a call that is difficult to ignore.", "Judgement reversed suggests hesitation to make a necessary decision or a harsh self-assessment that is preventing movement."),
    "The World": ("The World represents completion and integration. It points to reaching a threshold, recognizing progress, and stepping into a wider field of experience.", "The World reversed suggests unfinished business or one remaining step before a cycle can close."),
}

TARO_SUIT_MEANINGS = {
    "Wands": "Wands deal with initiative, enthusiasm, creativity, and the courage to act",
    "Cups": "Cups deal with feelings, belonging, relationships, and emotional recognition",
    "Swords": "Swords deal with thought, truth, communication, and decisions that need clear language",
    "Pentacles": "Pentacles deal with work, resources, reliability, and results that grow through consistent effort",
}

TARO_RANK_MEANINGS = {
    "Ace": "an opening or a new possibility",
    "Two": "a choice, exchange, or balancing of two sides",
    "Three": "development through participation, cooperation, or early results",
    "Four": "stability, protection, or a pattern becoming settled",
    "Five": "friction, lack, or a challenge that exposes what needs attention",
    "Six": "movement after difficulty, reciprocity, or the influence of the past",
    "Seven": "assessment, patience, and deciding what is worth continued effort",
    "Eight": "practice, momentum, or a situation becoming more focused",
    "Nine": "maturity, self-sufficiency, or the final stretch before completion",
    "Ten": "the full result of a pattern, including both its rewards and its burdens",
    "Page": "curiosity, learning, or a message that deserves attention",
    "Knight": "pursuit, movement, and the way an intention is acted upon",
    "Queen": "inward mastery, discernment, and a mature way of holding the suit's energy",
    "King": "outward mastery, responsibility, and directing the suit's energy deliberately",
}

TARO_MINOR_MEANINGS = {
    "Wands": {
        "Ace": ("a spark of energy and a promising reason to begin", "a promising start that is delayed or losing momentum"),
        "Two": ("planning the next move while looking beyond familiar territory", "hesitation about expanding beyond what already feels safe"),
        "Three": ("progress becoming visible after an initial effort", "progress slowed by weak planning or unrealistic expectations"),
        "Four": ("a stable milestone, welcome, or reason to celebrate", "instability beneath the surface or difficulty settling into a place"),
        "Five": ("competition, conflicting agendas, or productive friction", "conflict being avoided, or energy wasted on the wrong contest"),
        "Six": ("recognition, confidence, and an effort receiving a response", "a need for recognition that is not being met"),
        "Seven": ("holding your ground when your position is tested", "exhaustion, defensiveness, or uncertainty about what is worth defending"),
        "Eight": ("movement, messages, and events gathering speed", "delays, crossed signals, or activity without direction"),
        "Nine": ("persistence after experience has taught caution", "weariness or expecting another setback before it happens"),
        "Ten": ("a worthwhile effort becoming too heavy to carry alone", "a burden that must be delegated, reduced, or put down"),
        "Page": ("curiosity, a new interest, or news that awakens enthusiasm", "excitement without follow-through or a message that needs verification"),
        "Knight": ("bold pursuit and a willingness to act quickly", "impulsiveness, inconsistency, or energy that burns out too fast"),
        "Queen": ("warm confidence and the ability to encourage growth", "self-doubt, jealousy, or confidence that depends too much on approval"),
        "King": ("vision, leadership, and the confidence to direct an effort", "dominating the situation or pursuing ambition without enough restraint"),
    },
    "Cups": {
        "Ace": ("an emotional opening and a genuine capacity for connection", "feelings held back, emotional fatigue, or difficulty receiving care"),
        "Two": ("mutual recognition, attraction, or a meeting of equals", "misalignment, distance, or a connection that needs an honest conversation"),
        "Three": ("friendship, shared joy, and support found in community", "social strain, overindulgence, or a circle that does not feel fully trustworthy"),
        "Four": ("emotional withdrawal and the risk of overlooking an available opening", "renewed interest after a period of disengagement"),
        "Five": ("disappointment and attention fixed on what has been lost", "acceptance beginning to return after disappointment"),
        "Six": ("familiarity, memory, and a connection that feels known or sincere", "being held too tightly by the past or idealizing what used to be"),
        "Seven": ("many appealing possibilities that need to be judged realistically", "confusion lifting as priorities become clearer"),
        "Eight": ("leaving something emotionally incomplete because it no longer satisfies", "hesitation to walk away from a familiar but unfulfilling pattern"),
        "Nine": ("satisfaction and the enjoyment of a wish taking shape", "pleasure that does not fully satisfy or expectations becoming inflated"),
        "Ten": ("lasting emotional fulfillment, belonging, and a sense of shared happiness", "an ideal of happiness that may be masking strain or unmet expectations"),
        "Page": ("a sincere message, emotional curiosity, or a gentle new beginning", "emotional immaturity, mixed signals, or sensitivity that is hard to express"),
        "Knight": ("a heartfelt invitation and the wish to pursue what feels meaningful", "romanticizing the situation or making promises that lack grounding"),
        "Queen": ("empathy, emotional depth, and the ability to understand what is not said", "overwhelm, blurred boundaries, or feelings clouding judgment"),
        "King": ("emotional maturity and calm judgment under pressure", "feelings being tightly controlled, emotional inconsistency, or generosity without boundaries"),
    },
    "Swords": {
        "Ace": ("a clear realization, an honest conversation, or a decisive new idea", "confusion, a truth avoided, or a conversation that lacks clarity"),
        "Two": ("a difficult choice being held in suspension", "a delayed decision becoming harder to avoid"),
        "Three": ("painful truth, disappointment, or words that cannot be unheard", "healing after hurt, or pain that still needs acknowledgment"),
        "Four": ("rest, recovery, and the need to pause before acting again", "restlessness or returning to activity before recovery is complete"),
        "Five": ("conflict in which winning may cost more than expected", "a chance to de-escalate conflict or release resentment"),
        "Six": ("moving away from difficulty toward a calmer situation", "difficulty leaving a troubled pattern behind"),
        "Seven": ("strategy, discretion, or a situation where motives need scrutiny", "a concealed issue coming into view or a strategy that is not working"),
        "Eight": ("feeling trapped by assumptions, fear, or limited options", "recognizing that more choices exist than first appeared"),
        "Nine": ("worry, sleeplessness, or a fear becoming larger in private", "anxiety easing, or the need to seek support instead of carrying it alone"),
        "Ten": ("a painful ending that also makes continuation impossible", "slow recovery after an ending or reluctance to accept that a chapter is over"),
        "Page": ("alertness, questions, and the need to gather better information", "gossip, premature conclusions, or watching without understanding"),
        "Knight": ("direct action and a willingness to confront an issue", "recklessness, harsh words, or acting before the facts are clear"),
        "Queen": ("discernment, independence, and a preference for honest language", "bitterness, cutting communication, or judgment shaped by old hurt"),
        "King": ("clear analysis, standards, and decisions based on evidence", "cold reasoning, rigid judgment, or using intellect to dominate"),
    },
    "Pentacles": {
        "Ace": ("a tangible opening involving work, money, health, or long-term stability", "a practical opportunity delayed, missed, or poorly prepared for"),
        "Two": ("balancing competing demands while keeping resources in motion", "too many demands, weak budgeting, or difficulty keeping priorities balanced"),
        "Three": ("skill, collaboration, and work that improves through feedback", "poor coordination or effort that is not being valued properly"),
        "Four": ("holding resources carefully and protecting what has been built", "fear-driven control, overspending, or a need to loosen an overly tight grip"),
        "Five": ("financial strain, exclusion, or the feeling of facing difficulty alone", "recovery becoming possible when support is accepted"),
        "Six": ("a practical exchange of support, generosity, or fair compensation", "strings attached, unequal exchange, or giving more than is sustainable"),
        "Seven": ("patient assessment of whether steady effort is producing enough return", "impatience, poor return on effort, or a plan that needs adjustment"),
        "Eight": ("practice, craftsmanship, and improvement through disciplined repetition", "repetitive effort without improvement or standards slipping"),
        "Nine": ("self-sufficiency and the reward of choices made carefully over time", "dependence, financial insecurity, or appearances that cost too much to maintain"),
        "Ten": ("lasting security, family resources, and stability that extends beyond the present", "financial instability, family conflict over resources, or weak long-term planning"),
        "Page": ("a practical opportunity to learn, plan, or begin building", "procrastination, weak planning, or a practical lesson not yet taken seriously"),
        "Knight": ("reliability, patience, and progress made through consistent effort", "stagnation, stubborn routine, or effort continuing without a useful review"),
        "Queen": ("practical care, resourcefulness, and creating stability in daily life", "overextension, work-life imbalance, or neglecting your own practical needs"),
        "King": ("financial maturity, dependable leadership, and stewardship of resources", "materialism, inflexibility, or measuring success too narrowly"),
    },
}

TARO_THEME_SUIT_LENSES = {
    "spiritual connection": {
        "Wands": "For a spiritual-connection question, Wands point to taking initiative: going where shared interests are practiced rather than waiting for kindred people to appear.",
        "Cups": "For a spiritual-connection question, Cups point to emotional recognition and the feeling of being understood without having to perform a version of yourself.",
        "Swords": "For a spiritual-connection question, Swords point to honest conversation: naming your values clearly enough for compatible people to recognize them.",
        "Pentacles": "For a spiritual-connection question, Pentacles point to regular participation in a grounded community, not a single dramatic encounter.",
    },
    "relationship": {
        "Wands": "In a relationship question, Wands emphasize attraction, initiative, and whether both people are willing to make something happen.",
        "Cups": "In a relationship question, Cups emphasize emotional availability, affection, and whether the connection feels mutual.",
        "Swords": "In a relationship question, Swords emphasize communication, expectations, and truths that need to be said plainly.",
        "Pentacles": "In a relationship question, Pentacles emphasize reliability: what someone consistently does matters more than a promising moment.",
    },
    "financial": {
        "Wands": "In a money question, Wands emphasize initiative, a new venture, or the energy required to pursue an opportunity.",
        "Cups": "In a money question, Cups ask whether emotion, generosity, or the wish for security is shaping a financial choice.",
        "Swords": "In a money question, Swords emphasize research, negotiation, and a decision that needs accurate information.",
        "Pentacles": "In a money question, Pentacles are especially literal: they emphasize income, savings, assets, and progress built through repeatable habits.",
    },
    "career": {
        "Wands": "In a career question, Wands point to ambition, initiative, and work that gives you room to create or lead.",
        "Cups": "In a career question, Cups emphasize the human side of work: morale, collaboration, and whether the work feels meaningful.",
        "Swords": "In a career question, Swords emphasize planning, communication, and a decision that may require a candid conversation.",
        "Pentacles": "In a career question, Pentacles emphasize skill, dependable work, and results that can be demonstrated over time.",
    },
    "decision": {
        "Wands": "In a decision question, Wands ask which option has real momentum rather than momentary excitement.",
        "Cups": "In a decision question, Cups ask which option is emotionally honest and sustainable.",
        "Swords": "In a decision question, Swords ask what the evidence says once assumptions are separated from facts.",
        "Pentacles": "In a decision question, Pentacles ask which option is practical, stable, and workable in daily life.",
    },
}


def _taro_theme(question):
    words = set(re.findall(r"[a-z]+", question.lower()))
    for theme, keywords in TARO_THEMES:
        if words.intersection(keywords):
            return theme
    return "general"


def _offline_card_meaning(card, theme):
    name = card["name"]
    reversed_card = card["orientation"] == "reversed"
    if name in TARO_MAJOR_MEANINGS:
        return TARO_MAJOR_MEANINGS[name][1 if reversed_card else 0]

    rank, separator, suit = name.partition(" of ")
    if separator and rank in TARO_MINOR_MEANINGS.get(suit, {}):
        return TARO_MINOR_MEANINGS[suit][rank][1 if reversed_card else 0].capitalize() + "."
    return card["meaning"].capitalize() + "."


def _offline_spread_relationship(cards):
    suits = [card["name"].partition(" of ")[2] for card in cards]
    suits = [suit for suit in suits if suit in TARO_SUIT_MEANINGS]
    repeated_suit = next((suit for suit in TARO_SUIT_MEANINGS if suits.count(suit) >= 2), None)
    major_count = sum(card["name"] in TARO_MAJOR_MEANINGS for card in cards)
    reversed_count = sum(card["orientation"] == "reversed" for card in cards)
    observations = []
    if repeated_suit:
        observations.append(
            f"The repeated {repeated_suit} cards are the strongest thread in the spread. "
            f"{TARO_SUIT_MEANINGS[repeated_suit]}."
        )
    if major_count >= 2:
        observations.append(
            "With more than one Major Arcana card, this looks less like a passing mood and more like a larger pattern or turning point."
        )
    if reversed_count >= 2:
        observations.append(
            "Because two or more cards are reversed, the main issue is not simply outside circumstances. Some part of the pattern is blocked, delayed, or being handled indirectly."
        )
    elif reversed_count == 0:
        observations.append(
            "All three cards are upright, so the spread reads as relatively direct: the next step is more about acting on what is visible than uncovering a hidden obstacle."
        )
    return " ".join(observations)


def _offline_financial_action(card):
    actions = {
        "Ace of Pentacles": "take a tangible opportunity seriously and make a concrete plan for it",
        "Four of Pentacles": "review whether fear is making you grip money too tightly in some places while handling it reactively in others",
        "Five of Pentacles": "seek practical support instead of treating financial strain as something you must solve in isolation",
        "Seven of Pentacles": "review which efforts are actually producing a return and stop feeding the ones that are not",
        "Eight of Pentacles": "build a skill or repeatable habit that improves your earning power over time",
        "Nine of Pentacles": "favor independence and choices that strengthen your long-term stability",
        "Ten of Pentacles": "think beyond a quick gain and plan for durable security",
        "King of Cups": "notice where emotion, generosity, or the wish to feel secure is influencing your money decisions",
    }
    advice = actions.get(card["name"])
    if advice:
        return advice
    return f'use the lesson of {card["name"]} as a practical test before making your next money decision'


def _offline_taro_reading(question, cards):
    theme = _taro_theme(question)
    details = []
    position_context = {
        "Background": "Here it describes the experience or expectation that shaped the question.",
        "Present": "This is the part of the situation asking for your attention now.",
        "Direction": "This is the approach most likely to move the situation forward.",
    }
    for card in cards:
        details.append(
            f'{card["position"]} - {card["name"]}{card["orientation_suffix"]}\n'
            f'{_offline_card_meaning(card, theme)} {position_context[card["position"]]}'
        )

    first, second, third = cards
    relationship = _offline_spread_relationship(cards)
    if relationship:
        relationship = "\n\nHow the cards work together\n" + relationship
    if theme == "spiritual connection":
        closing = (
            f'{first["name"]} suggests that your question begins with a real desire for belonging, not simply '
            f'with wanting more acquaintances. {second["name"]} brings the focus to the kind of connection '
            f'that can feel familiar, reciprocal, or rooted in shared experience. {third["name"]} says the next '
            f'step is clarity: speak naturally but plainly about what matters to you, and spend time in settings '
            f'where those values can become visible in conversation. The cards favor recognition through honest '
            f'exchange over waiting for an unmistakable sign.'
        )
    elif theme == "financial":
        closing = (
            f'The direct answer is that these cards do not point to sudden riches, but they do leave room for '
            f'meaningful financial improvement. {first["name"]} suggests that the question is partly shaped by '
            f'how money feels, not only by the numbers. {second["name"]} says to {_offline_financial_action(second)}. '
            f'{third["name"]} adds a caution: {_offline_financial_action(third)}. This spread favors wealth built '
            f'gradually through clearer habits and better judgment, not a windfall.'
        )
    elif theme == "career":
        closing = (
            f'{first["name"]} shows the work pattern that brought you to the question. {second["name"]} identifies '
            f'what is active now, while {third["name"]} describes the most useful next move. Look for the step '
            f'you can carry out consistently and evaluate honestly rather than relying on a promised outcome.'
        )
    elif theme == "relationship":
        closing = (
            f'{first["name"]} shows the expectation or history behind the question. {second["name"]} describes '
            f'the relationship dynamic that matters now, and {third["name"]} shows what should guide your next '
            f'conversation or choice. Look for consistent behavior that matches the direction card rather than '
            f'trying to force certainty from a single moment.'
        )
    else:
        closing = (
            f'{first["name"]} shows where the question began, but {second["name"]} is the center of the reading '
            f'because it describes what is active now. {third["name"]} is the response: use that direction card '
            f'as a specific test for your next choice or conversation rather than waiting for certainty.'
        )
    return (
        f'Question: {question}\n\n'
        f'{TARO_THEME_OPENERS[theme]}\n\n'
        + "\n\n".join(details)
        + relationship
        + f'\n\nOverall reading\n{closing} This is reflective guidance, not a factual prediction or professional advice.'
    )


@app.route("/api/taro/interpret", methods=["POST"])
@login_required
def api_taro_interpret():
    body = request.get_json(silent=True) or {}
    question = str(body.get("question", "")).strip()
    raw_cards = body.get("cards")
    if not question:
        return jsonify({"error": "Please type a question before drawing the cards."}), 400
    if len(question) > 240:
        return jsonify({"error": "Please keep the question under 240 characters."}), 400
    if not isinstance(raw_cards, list) or len(raw_cards) != 3:
        return jsonify({"error": "A three-card reading is required."}), 400

    cards = []
    expected_positions = ("Background", "Present", "Direction")
    for index, raw_card in enumerate(raw_cards):
        if not isinstance(raw_card, dict):
            return jsonify({"error": "The card reading is invalid."}), 400
        name = str(raw_card.get("name", "")).strip()[:80]
        meaning = str(raw_card.get("meaning", "")).strip()[:300]
        orientation = "reversed" if raw_card.get("isReversed") else "upright"
        if not name or not meaning:
            return jsonify({"error": "The card reading is incomplete."}), 400
        cards.append({
            "position": expected_positions[index],
            "name": name,
            "meaning": meaning,
            "orientation": orientation,
            "orientation_suffix": " (reversed)" if orientation == "reversed" else "",
        })

    fallback = _offline_taro_reading(question, cards)
    offline_only = os.environ.get("OPENAI_TARO_OFFLINE_ONLY", "").strip().lower() in {"1", "true", "yes", "on"}
    api_key = "" if offline_only else os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return jsonify({
            "interpretation": fallback,
            "source": "offline",
            "note": "Using the built-in local interpretation.",
        })

    spread = "\n".join(
        f'- {card["position"]}: {card["name"]} ({card["orientation"]}). Base meaning: {card["meaning"]}'
        for card in cards
    )
    try:
        response = requests.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": os.environ.get("OPENAI_TARO_MODEL", "gpt-5.4-mini"),
                "instructions": TARO_READER_INSTRUCTIONS,
                "input": f"Client question: {question}\n\nThree-card spread:\n{spread}",
                "max_output_tokens": 900,
            },
            timeout=45,
        )
        response.raise_for_status()
        interpretation = _taro_output_text(response.json())
        if not interpretation:
            raise ValueError("OpenAI returned an empty interpretation.")
        return jsonify({"interpretation": interpretation, "source": "openai"})
    except Exception as exc:
        logger.exception("Could not generate OpenAI Tarot interpretation")
        diagnostic = f"{type(exc).__name__}: {exc}"
        if len(diagnostic) > 240:
            diagnostic = diagnostic[:237] + "..."
        return jsonify({
            "interpretation": fallback,
            "source": "offline",
            "note": "The AI interpretation was unavailable, so the built-in interpretation is shown.",
            "diagnostic": diagnostic if app.debug else None,
        })


@app.route("/tickets")
@login_required
def tickets_page():
    trigger_ticket_cleanup_async()
    lotto_type = _resolve_lotto_arg("MM")

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
        max_ticket_permutations=MAX_TICKET_PERMUTATIONS,
        app_build=APP_BUILD,
    ))
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/api/build")
def api_build():
    return jsonify({"build": APP_BUILD})


@app.route("/api/ticket_expectations")
@login_required
def api_ticket_expectations():
    lotto_type = _resolve_lotto_arg("MM")
    draw_date = request.args.get("draw_date")

    _, latest_str = db.get_date_bounds(lotto_type)
    draws = db.get_all_draws(lotto_type)
    if not latest_str or not draws:
        return jsonify({"error": "No draw history available"}), 404

    rows = db_forecast.get_forecast_bands(lotto_type, latest_str, latest_str, FORECAST_MODEL)
    corridors = []
    for row in rows:
        corridors.append({
            "set": int(row["SetNumber"]),
            "safe_low": row["SafeLow"],
            "safe_high": row["SafeHigh"],
            "hot_low": row["HotLow"],
            "hot_high": row["HotHigh"],
        })

    return jsonify({
        "lotto": lotto_type,
        "last_draw_date": latest_str,
        "draw_date": draw_date or latest_str,
        "directions": _first_projected_match(lotto_type, "directions", draws),
        "jumps": _first_projected_match(lotto_type, "jumps", draws),
        "corridors": corridors,
    })


@app.route("/api/tickets")
@login_required
def api_tickets_get():
    trigger_ticket_cleanup_async()
    lotto_type = _resolve_lotto_arg("MM")
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
    lotto_type = _resolve_lotto_payload(data, "MM")
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
        lotto_type = _resolve_lotto_payload(data, "MM")
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

        permutation_count = 1
        for bucket in parsed_buckets:
            permutation_count *= len(bucket)
        if permutation_count > MAX_TICKET_PERMUTATIONS:
            return jsonify({
                "error": (
                    f"That would generate {permutation_count:,} permutations. "
                    f"Please reduce the lists to {MAX_TICKET_PERMUTATIONS:,} or fewer combinations."
                ),
                "permutations": permutation_count,
                "max_permutations": MAX_TICKET_PERMUTATIONS,
            }), 400

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
    except sqlite3.OperationalError as exc:
        if "locked" in str(exc).lower():
            logger.warning("Ticket permutation save hit a locked database")
            return jsonify({"error": "Could not generate tickets on the server: database is busy. Please try again in a moment."}), 503
        logger.exception("Could not generate ticket permutations")
        return jsonify({"error": f"Could not generate tickets on the server: {exc}"}), 500
    except Exception as exc:
        logger.exception("Could not generate ticket permutations")
        return jsonify({"error": f"Could not generate tickets on the server: {exc}"}), 500


@app.route("/api/manual_draw", methods=["POST"])
@login_required
def api_manual_draw():
    try:
        data = request.get_json(silent=True) or {}
        lotto_type = _resolve_lotto_payload(data, "MM")
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

        db.mark_manual_draw(lotto_type, draw_date)
        trigger_forecast_refresh_async(lotto_type)

        return jsonify({
            "saved": True,
            "updated": bool(existing),
            "lotto": lotto_type,
            "draw_date": draw_date,
            "numbers": parsed,
        }), 201
    except Exception as exc:
        logger.exception("Could not save manual draw")
        return jsonify({"error": f"Could not save the winning numbers on the server: {exc}"}), 500


@app.route("/api/tickets/<int:ticket_id>", methods=["DELETE", "POST"])
@login_required
def api_tickets_delete(ticket_id):
    deleted = db_ticket_sim.delete_ticket(ticket_id)
    if not deleted:
        # Treat stale rows as effectively deleted so the UI can refresh cleanly.
        return jsonify({"deleted": ticket_id, "missing": True})
    return jsonify({"deleted": ticket_id})


@app.route("/api/tickets/optional", methods=["DELETE", "POST"])
@login_required
def api_tickets_delete_optional():
    data = request.get_json(silent=True) or {}
    lotto_type = _resolve_lotto_payload(data, "MM")
    draw_date = (data.get("draw_date") or "").strip()
    if not draw_date:
        return jsonify({"error": "draw_date required"}), 400
    try:
        date.fromisoformat(draw_date)
    except ValueError:
        return jsonify({"error": "Invalid draw date"}), 400
    deleted = db_ticket_sim.delete_optional_tickets(lotto_type, draw_date)
    return jsonify({"deleted": deleted})


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
    lotto_type  = _resolve_lotto_arg("CA")
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
    lotto_type   = _resolve_lotto_arg("CA")
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
    lotto_type = _resolve_lotto_arg("CA")
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
    lotto_type = _resolve_lotto_arg("CA")
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
    lotto_type  = _resolve_lotto_arg("CA")
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
    lotto_type   = _resolve_lotto_arg("CA")
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
    lotto_type   = _resolve_lotto_arg("CA")
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
    lotto_type = _resolve_lotto_arg("CA")
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
    lotto_type = _resolve_lotto_arg("CA")
    mode = request.args.get("mode", "directions")
    set_number = request.args.get("set", "1", type=int)
    if mode not in {"directions", "jumps", "overdue", "weighted_overdue", "set_overdue"}:
        mode = "directions"
    return render_template(
        "gaps.html",
        lotto_type=lotto_type,
        gap_mode=mode,
        set_overdue_set=set_number if set_number in {1, 2, 3, 4, 5, 6} else 1,
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
    lotto_type = _resolve_lotto_arg("CA")
    mode = request.args.get("mode", "directions")
    set_number = request.args.get("set", "1", type=int)
    draws = db.get_all_draws(lotto_type)
    if not draws:
        return jsonify([])
    if mode == "set_overdue":
        return jsonify(_set_overdue_numbers(lotto_type, draws, set_number or 1))
    if mode == "weighted_overdue":
        return jsonify(_weighted_overdue_numbers(lotto_type, draws))
    if mode == "overdue":
        return jsonify(_overdue_numbers(lotto_type, draws))
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


def _refresh_after_scrape_pass(summary: dict[str, int]) -> None:
    """
    Keep derived data in sync when the background scraper inserts new draws.

    Render relies on a persistent database, so newly scraped draws need
    forecast and Stage 3 refresh without waiting for a later request to
    discover the new dates.
    """
    if not summary:
        return

    changed = [lt for lt, count in summary.items() if count]
    if not changed:
        return

    logger.info("Background scrape inserted new draws for %s; refreshing derived data", changed)

    for lt in changed:
        try:
            _refresh_forecasts_for_lotto(lt)
        except Exception as exc:
            logger.warning("%s forecast refresh after scrape pass failed: %s", lt, exc)

    try:
        _populate_selections()
    except Exception as exc:
        logger.warning("Selection refresh after scrape pass failed: %s", exc)


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
        run_background_scraper = _env_flag(
            "LOTTO_BACKGROUND_SCRAPER",
            not is_render_runtime,
        )
        if run_background_scraper:
            scraper.start_background_scraper(on_pass_complete=_refresh_after_scrape_pass)
        elif is_render_runtime:
            logger.info("Background scraper disabled on Render web runtime.")

        _runtime_initialized = True


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

initialize_runtime()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
