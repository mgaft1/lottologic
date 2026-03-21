"""
app.py  --  Lotto Viewer Phase 1
Window slicing by calendar days. Mode A navigation only.
PB/PD: effective_days = round(days * 0.6).
Background scraper fills missing draws; never blocks rendering.
"""

import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

# db_forecast and forecast live in ../data/ relative to this file (files/app.py)
sys.path.insert(0, str(Path(__file__).parent.parent / "data"))

from flask import Flask, render_template, request, jsonify, session, redirect, url_for
import db
import db_forecast
import db_selection
import db_links
import selection
import scraper
import gap_engine
import links_fetcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get("LOTTO_SECRET", "change-me-in-production-32chars!!")

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
    return render_template(
        "gaps.html",
        lotto_type=lotto_type,
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
    draws = db.get_all_draws(lotto_type)
    if not draws:
        return jsonify([])
    matches = gap_engine.find_matches(draws)
    return jsonify(matches)


# ---------------------------------------------------------------------------
# YouTube Links page
# ---------------------------------------------------------------------------

CATEGORIES = ["music", "cooking", "baking"]


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


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    xlsx = Path(__file__).parent.parent / "data" / "Lotto.xlsx"
    if not Path(db.DB_PATH).exists():
        logger.info("Initialising database...")
        db.init_db()
        summary = db.ingest_xlsx(str(xlsx))
        logger.info("Ingested: %s", summary)

    # Stage 2: ensure ForecastPredictions table exists, then backfill any
    # lotto types that have no forecast rows yet.
    db_forecast.init_forecast_schema()
    _backfill_missing()

    # Stage 3
    db_selection.init_selection_schema()
    _populate_selections()

    # Links
    db_links.init_links_schema()

    scraper.start_background_scraper()
    app.run(debug=True, host="0.0.0.0", port=5000)
