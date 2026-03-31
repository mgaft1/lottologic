"""
db_forecast.py  --  Stage 2 data access layer (additive)

Provides the DAL methods required by BackfillPredictions and the
forecast API endpoint.  This module is SEPARATE from db.py and does
not modify it.  Both modules share the same DB_PATH / _conn() pattern.

New functions:
    init_forecast_schema()           -- run forecast_schema.sql once
    load_all_history(lotto_type)     -- mirrors LoadAllHistoryOnce
    get_draw_index_before(...)       -- mirrors getDrawIndexBefore
    get_draw_dates(lotto_type)       -- all draw dates for a type
    get_draw_dates_after(...)        -- dates >= cutoff (for incremental runs)
    persist_forecast_bands(...)      -- mirrors PersistForecastBands + sp
    get_forecast_bands(...)          -- viewer API query
    forecast_exists(...)             -- mirrors ForecastExists (guard)
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Optional
import os

# Resolve DB path identically to db.py so both modules share the same file.
def _resolve_db_path() -> str:
    """
    Locate lotto.db regardless of whether this file sits in files/ or data/.
    Checks: same directory as this file, then ../data/ relative to this file.
    """
    if os.environ.get("LOTTO_DB"):
        return os.environ["LOTTO_DB"]
    if os.environ.get("LOTTO_DB_DIR"):
        return str(Path(os.environ["LOTTO_DB_DIR"]) / "lotto.db")
    if os.environ.get("RENDER_DISK_PATH"):
        return str(Path(os.environ["RENDER_DISK_PATH"]) / "lotto.db")
    here = Path(__file__).parent
    candidates = [
        here / "lotto.db",                  # data/lotto.db  (when __file__ is data/)
        here.parent / "data" / "lotto.db",  # ../data/lotto.db (when __file__ is files/)
    ]
    for p in candidates:
        if p.exists():
            return str(p)
    # Fall back to data/ sibling — will surface a clear error on connect
    return str(here.parent / "data" / "lotto.db")

DB_PATH = os.environ.get("LOTTO_DB", _resolve_db_path())

def _journal_mode() -> str:
    mode = os.environ.get("LOTTO_SQLITE_JOURNAL_MODE", "DELETE").upper()
    return mode if mode in {"DELETE", "WAL", "TRUNCATE", "PERSIST", "MEMORY", "OFF"} else "DELETE"


@contextmanager
def _conn():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute(f"PRAGMA journal_mode={_journal_mode()}")
    con.execute("PRAGMA foreign_keys=ON")
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Schema initialisation  (idempotent -- CREATE TABLE IF NOT EXISTS)
# ---------------------------------------------------------------------------

def init_forecast_schema() -> None:
    """Apply forecast_schema.sql to the existing database."""
    here = Path(__file__).parent
    candidates = [
        here / "forecast_schema.sql",                  # data/ location
        here.parent / "data" / "forecast_schema.sql",  # files/ loading, schema in data/
    ]
    schema = next((p for p in candidates if p.exists()), candidates[0])
    with _conn() as con:
        con.executescript(schema.read_text(encoding="utf-8-sig"))


# ---------------------------------------------------------------------------
# load_all_history
# Mirrors: List<lottoVal> allHistory = ld.LoadAllHistoryOnce(state)
#
# Returns ALL draws for this lotto type, ordered by DrawIndex ascending.
# Keys: DrawIndex, DrawDate, Nbr1..Nbr6
# ---------------------------------------------------------------------------

def load_all_history(lotto_type: str) -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            """SELECT DrawIndex, DrawDate, Nbr1, Nbr2, Nbr3, Nbr4, Nbr5, Nbr6
               FROM DrawHistory
               WHERE LottoType = ?
               ORDER BY DrawIndex""",
            (lotto_type,),
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# get_draw_index_before
# Mirrors: lda.getDrawIndexBefore(state, forecastDate)
#
# Returns the DrawIndex of the most recent draw with DrawDate < forecastDate.
# Returns 0 if none exists (caller skips with `if as_of_index <= 0: continue`).
# ---------------------------------------------------------------------------

def get_draw_index_before(lotto_type: str, forecast_date: str) -> int:
    """
    DrawIndex of the draw immediately before forecast_date (strict <).

    Returns 0 if no such draw exists OR if the result is DrawIndex=1.
    DrawIndex=1 → ProcessDrawingDate(1) → UpperBound=0 → empty window.
    The C# getDrawIndexBefore returns 0 in this case by convention;
    the BackfillPredictions guard `if (asOfDrawIndex <= 0) continue` skips it.
    """
    with _conn() as con:
        row = con.execute(
            """SELECT DrawIndex FROM DrawHistory
               WHERE LottoType = ? AND DrawDate < ?
               ORDER BY DrawDate DESC
               LIMIT 1""",
            (lotto_type, forecast_date),
        ).fetchone()
    if not row:
        return 0
    idx = int(row["DrawIndex"])
    return idx if idx > 1 else 0


# ---------------------------------------------------------------------------
# get_draw_dates / get_draw_dates_after
# Mirrors: List<DateTime> drawDates = GetDrawDates(state, daysBack)
# ---------------------------------------------------------------------------

def get_draw_dates(lotto_type: str) -> list[str]:
    """Return all draw dates for lotto_type, ascending."""
    with _conn() as con:
        rows = con.execute(
            """SELECT DrawDate FROM DrawHistory
               WHERE LottoType = ?
               ORDER BY DrawDate""",
            (lotto_type,),
        ).fetchall()
    return [r["DrawDate"] for r in rows]


def get_draw_dates_after(lotto_type: str, cutoff: str) -> list[str]:
    """
    Return draw dates >= cutoff, ascending.
    Use for incremental nightly runs: cutoff = date of last known
    forecast row for this (lotto_type, model_version).
    """
    with _conn() as con:
        rows = con.execute(
            """SELECT DrawDate FROM DrawHistory
               WHERE LottoType = ? AND DrawDate >= ?
               ORDER BY DrawDate""",
            (lotto_type, cutoff),
        ).fetchall()
    return [r["DrawDate"] for r in rows]


# ---------------------------------------------------------------------------
# forecast_exists
# Mirrors: lda.ForecastExists(state, latestDrawDate, MODEL_VERSION)
# ---------------------------------------------------------------------------

def forecast_exists(lotto_type: str, draw_date: str, model_version: str) -> bool:
    with _conn() as con:
        row = con.execute(
            """SELECT 1 FROM ForecastPredictions
               WHERE LottoType = ? AND DrawDate = ? AND ModelVersion = ?
               LIMIT 1""",
            (lotto_type, draw_date, model_version),
        ).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# persist_forecast_bands
# Mirrors: PersistForecastBands DAL method + sp_PersistForecastBands SP.
#
# Inserts 6 rows (SetNumber 1..6) for (LottoType, DrawDate, ModelVersion).
# Idempotent: INSERT OR IGNORE via the UQ_ForecastPredictions constraint,
# which mirrors the WHERE NOT EXISTS guard in sp_PersistForecastBands.
#
# Parameters match BackfillPredictions exactly:
#   safe_min, safe_max : list[int]         -- indexed 1..6; index 0 unused
#   hot_min,  hot_max  : list[int|None]    -- indexed 1..6; index 0 unused
# ---------------------------------------------------------------------------

def persist_forecast_bands(
    lotto_type: str,
    draw_date: str,
    model_version: str,
    safe_min: list[int],
    safe_max: list[int],
    hot_min:  list[Optional[int]],
    hot_max:  list[Optional[int]],
) -> None:
    """
    Persist 6 band rows for one (LottoType, DrawDate, ModelVersion).
    INSERT OR IGNORE: safe to call repeatedly.
    """
    with _conn() as con:
        for set_number in range(1, 7):
            con.execute(
                """INSERT OR IGNORE INTO ForecastPredictions
                   (LottoType, DrawDate, SetNumber,
                    SafeLow, SafeHigh, HotLow, HotHigh,
                    ModelVersion)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    lotto_type,
                    draw_date,
                    set_number,
                    safe_min[set_number],
                    safe_max[set_number],
                    hot_min[set_number],
                    hot_max[set_number],
                    model_version,
                ),
            )


# ---------------------------------------------------------------------------
# get_forecast_bands
# Viewer API query: return SAFE/HOT bands for all sets in a date window.
# Returns list of dicts, one per (DrawDate, SetNumber).
# ---------------------------------------------------------------------------

def get_forecast_bands(
    lotto_type: str,
    date_from: str,
    date_to: str,
    model_version: str,
) -> list[dict]:
    """
    Return forecast band rows for the viewer's date window.

    Columns: DrawDate, SetNumber, SafeLow, SafeHigh, HotLow, HotHigh
    Ordered by DrawDate ASC, SetNumber ASC.
    """
    with _conn() as con:
        rows = con.execute(
            """SELECT DrawDate, SetNumber, SafeLow, SafeHigh, HotLow, HotHigh
               FROM ForecastPredictions
               WHERE LottoType   = ?
                 AND DrawDate   >= ?
                 AND DrawDate   <= ?
                 AND ModelVersion = ?
               ORDER BY DrawDate, SetNumber""",
            (lotto_type, date_from, date_to, model_version),
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# get_last_forecast_date
# Used by incremental runner to know where backfill left off.
# ---------------------------------------------------------------------------

def get_last_forecast_date(lotto_type: str, model_version: str) -> Optional[str]:
    """
    Return the most recent DrawDate already persisted for
    (lotto_type, model_version), or None if none exist.
    """
    with _conn() as con:
        row = con.execute(
            """SELECT MAX(DrawDate) AS last_date FROM ForecastPredictions
               WHERE LottoType = ? AND ModelVersion = ?""",
            (lotto_type, model_version),
        ).fetchone()
    return row["last_date"] if row and row["last_date"] else None


# ---------------------------------------------------------------------------
# get_forecast_chart_data
# Chart API query: joins ForecastPredictions with DrawHistory to add
# ActualValue (the real drawn number) for each (DrawDate, SetNumber).
#
# Returns one row per (DrawDate, SetNumber) in [date_from, date_to].
# Columns: DrawIndex, DrawDate, SetNumber,
#          ActualValue, SafeLow, SafeHigh, HotLow, HotHigh
# Ordered by DrawDate ASC, SetNumber ASC.
#
# ActualValue is NULL for dates where no DrawHistory row exists
# (e.g. future forecast dates).
# ---------------------------------------------------------------------------

def get_forecast_chart_data(
    lotto_type:    str,
    date_from:     str,
    date_to:       str,
    model_version: str,
) -> list[dict]:
    """
    Return forecast bands joined with actual drawn values for chart rendering.

    5 series per set:
        ActualValue  -> black solid line
        SafeHigh     -> teal dashed upper
        SafeLow      -> teal dashed lower
        HotHigh      -> red dashed upper
        HotLow       -> red dashed lower
    """
    with _conn() as con:
        rows = con.execute(
            """SELECT
                   dh.DrawIndex,
                   fp.DrawDate,
                   fp.SetNumber,
                   CASE fp.SetNumber
                       WHEN 1 THEN dh.Nbr1
                       WHEN 2 THEN dh.Nbr2
                       WHEN 3 THEN dh.Nbr3
                       WHEN 4 THEN dh.Nbr4
                       WHEN 5 THEN dh.Nbr5
                       WHEN 6 THEN dh.Nbr6
                   END AS ActualValue,
                   fp.SafeLow,
                   fp.SafeHigh,
                   fp.HotLow,
                   fp.HotHigh
               FROM ForecastPredictions fp
               JOIN DrawHistory dh
                   ON  dh.LottoType = fp.LottoType
                   AND dh.DrawDate  = fp.DrawDate
               WHERE fp.LottoType    = ?
                 AND fp.DrawDate    >= ?
                 AND fp.DrawDate    <= ?
                 AND fp.ModelVersion = ?
               ORDER BY fp.DrawDate, fp.SetNumber""",
            (lotto_type, date_from, date_to, model_version),
        ).fetchall()
    return [dict(r) for r in rows]
