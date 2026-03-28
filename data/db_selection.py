"""
db_selection.py  --  Stage 3 data access layer (additive)

Never touches DrawHistory or ForecastPredictions.
Shares the same DB_PATH resolution pattern as db_forecast.py.

Functions:
    init_selection_schema()          -- apply selection_schema.sql once
    persist_combinations(...)        -- INSERT OR IGNORE ranked combos
    get_combinations(...)            -- fetch combos for a draw
    combinations_exist(...)          -- guard: already persisted?
    get_last_selection_date(...)     -- incremental runner support
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Optional
import os


def _resolve_db_path() -> str:
    if os.environ.get("LOTTO_DB"):
        return os.environ["LOTTO_DB"]
    if os.environ.get("LOTTO_DB_DIR"):
        return str(Path(os.environ["LOTTO_DB_DIR"]) / "lotto.db")
    if os.environ.get("RENDER_DISK_PATH"):
        return str(Path(os.environ["RENDER_DISK_PATH"]) / "lotto.db")
    here = Path(__file__).parent
    candidates = [
        here / "lotto.db",
        here.parent / "data" / "lotto.db",
    ]
    for p in candidates:
        if p.exists():
            return str(p)
    return str(here.parent / "data" / "lotto.db")


DB_PATH = os.environ.get("LOTTO_DB", _resolve_db_path())


@contextmanager
def _conn():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
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
# Schema
# ---------------------------------------------------------------------------

def init_selection_schema() -> None:
    """Apply selection_schema.sql to the existing database (idempotent)."""
    here = Path(__file__).parent
    candidates = [
        here / "selection_schema.sql",
        here.parent / "data" / "selection_schema.sql",
    ]
    schema = next((p for p in candidates if p.exists()), candidates[0])
    with _conn() as con:
        con.executescript(schema.read_text(encoding="utf-8-sig"))


# ---------------------------------------------------------------------------
# persist_combinations
# Mirrors the idempotency pattern from db_forecast.persist_forecast_bands.
# INSERT OR IGNORE on the composite PK — safe to call repeatedly.
# ---------------------------------------------------------------------------

def persist_combinations(
    combinations: list,   # list of Combination dataclass instances
) -> None:
    """
    Persist a list of Combination objects.
    Each Combination must have: lotto_type, draw_date, combination_id,
    nbr1-6, score, selection_reason attributes.
    INSERT OR IGNORE — idempotent.
    """
    with _conn() as con:
        for c in combinations:
            con.execute(
                """INSERT OR IGNORE INTO CandidateCombinations
                   (CombinationId, LottoType, DrawDate,
                    Nbr1, Nbr2, Nbr3, Nbr4, Nbr5, Nbr6,
                    Score, SelectionReason, ModelVersion)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    c.combination_id,
                    c.lotto_type,
                    c.draw_date,
                    c.nbr1, c.nbr2, c.nbr3, c.nbr4, c.nbr5, c.nbr6,
                    c.score,
                    c.selection_reason,
                    # ModelVersion is not on Combination dataclass — pass via closure
                    getattr(c, "_model_version", "WF_v4_baseline"),
                ),
            )


def persist_combinations_versioned(
    combinations: list,
    model_version: str,
) -> None:
    """
    Persist combinations with an explicit model_version.
    Preferred over persist_combinations when model version is known at call site.
    """
    with _conn() as con:
        for c in combinations:
            con.execute(
                """INSERT OR IGNORE INTO CandidateCombinations
                   (CombinationId, LottoType, DrawDate,
                    Nbr1, Nbr2, Nbr3, Nbr4, Nbr5, Nbr6,
                    Score, SelectionReason, ModelVersion)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    c.combination_id,
                    c.lotto_type,
                    c.draw_date,
                    c.nbr1, c.nbr2, c.nbr3, c.nbr4, c.nbr5, c.nbr6,
                    c.score,
                    c.selection_reason,
                    model_version,
                ),
            )


# ---------------------------------------------------------------------------
# get_combinations
# ---------------------------------------------------------------------------

def get_combinations(
    lotto_type:    str,
    draw_date:     str,
    model_version: str,
) -> list[dict]:
    """
    Return all ranked combinations for (LottoType, DrawDate, ModelVersion),
    ordered by CombinationId ascending (i.e. Score descending — rank order).
    """
    with _conn() as con:
        rows = con.execute(
            """SELECT CombinationId, LottoType, DrawDate,
                      Nbr1, Nbr2, Nbr3, Nbr4, Nbr5, Nbr6,
                      Score, SelectionReason
               FROM CandidateCombinations
               WHERE LottoType = ? AND DrawDate = ? AND ModelVersion = ?
               ORDER BY CombinationId""",
            (lotto_type, draw_date, model_version),
        ).fetchall()
    return [dict(r) for r in rows]


def get_combinations_window(
    lotto_type:    str,
    date_from:     str,
    date_to:       str,
    model_version: str,
) -> list[dict]:
    """
    Return combinations for all draw dates in [date_from, date_to].
    Ordered by DrawDate ASC, CombinationId ASC.
    """
    with _conn() as con:
        rows = con.execute(
            """SELECT CombinationId, LottoType, DrawDate,
                      Nbr1, Nbr2, Nbr3, Nbr4, Nbr5, Nbr6,
                      Score, SelectionReason
               FROM CandidateCombinations
               WHERE LottoType = ? AND DrawDate >= ? AND DrawDate <= ?
                 AND ModelVersion = ?
               ORDER BY DrawDate, CombinationId""",
            (lotto_type, date_from, date_to, model_version),
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Guard / incremental helpers
# ---------------------------------------------------------------------------

def combinations_exist(
    lotto_type:    str,
    draw_date:     str,
    model_version: str,
) -> bool:
    with _conn() as con:
        row = con.execute(
            """SELECT 1 FROM CandidateCombinations
               WHERE LottoType = ? AND DrawDate = ? AND ModelVersion = ?
               LIMIT 1""",
            (lotto_type, draw_date, model_version),
        ).fetchone()
    return row is not None


def get_last_selection_date(
    lotto_type:    str,
    model_version: str,
) -> Optional[str]:
    """Return the most recent DrawDate with persisted combinations, or None."""
    with _conn() as con:
        row = con.execute(
            """SELECT MAX(DrawDate) AS last_date FROM CandidateCombinations
               WHERE LottoType = ? AND ModelVersion = ?""",
            (lotto_type, model_version),
        ).fetchone()
    return row["last_date"] if row and row["last_date"] else None
