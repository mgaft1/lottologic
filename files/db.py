"""
db.py  --  Phase 1 data layer
SQLite storage, SQL Server-compatible schema.
Window slicing is by calendar days, not draw count.
"""

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

def _resolve_db_path() -> str:
    if os.environ.get("LOTTO_DB"):
        return os.environ["LOTTO_DB"]
    if os.environ.get("LOTTO_DB_DIR"):
        return str(Path(os.environ["LOTTO_DB_DIR"]) / "lotto.db")
    if os.environ.get("RENDER_DISK_PATH"):
        return str(Path(os.environ["RENDER_DISK_PATH"]) / "lotto.db")
    return str(Path(__file__).resolve().parent.parent / "data" / "lotto.db")


DB_PATH = _resolve_db_path()


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


def init_db():
    here = Path(__file__).resolve().parent
    candidates = [
        here / "schema.sql",
        here.parent / "files" / "schema.sql",
        here.parent / "db" / "schema.sql",
    ]
    schema = next((p for p in candidates if p.exists()), candidates[0])
    with _conn() as con:
        con.executescript(schema.read_text())


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def ingest_xlsx(xlsx_path: str) -> dict:
    """Load draws from Lotto.xlsx. Idempotent on (LottoType, DrawDate)."""
    import openpyxl
    wb = openpyxl.load_workbook(xlsx_path, data_only=True)
    ws = wb["lotto"]
    inserted = {}
    with _conn() as con:
        for row in ws.iter_rows(min_row=2, values_only=True):
            id_, date_val, n1, n2, n3, n4, n5, n6, record_type, state, draw_index = row
            if not state or not draw_index:
                continue
            lt = state.strip()
            draw_date = str(date_val)[:10]
            cur = con.execute(
                """INSERT OR IGNORE INTO DrawHistory
                   (Id,LottoType,DrawDate,DrawIndex,Nbr1,Nbr2,Nbr3,Nbr4,Nbr5,Nbr6)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (int(id_), lt, draw_date, int(draw_index),
                 int(n1), int(n2), int(n3), int(n4), int(n5),
                 int(n6) if n6 is not None else None),
            )
            inserted[lt] = inserted.get(lt, 0) + cur.rowcount
    return inserted


def insert_draw(lotto_type: str, draw_date: str,
                n1: int, n2: int, n3: int, n4: int, n5: int,
                n6) -> bool:
    """
    Insert one scraped draw. Assigns DrawIndex = max+1.
    Idempotent on (LottoType, DrawDate). Returns True if inserted.
    """
    with _conn() as con:
        exists = con.execute(
            "SELECT 1 FROM DrawHistory WHERE LottoType=? AND DrawDate=?",
            (lotto_type, draw_date)
        ).fetchone()
        if exists:
            return False

        next_index = (con.execute(
            "SELECT MAX(DrawIndex) FROM DrawHistory WHERE LottoType=?",
            (lotto_type,)
        ).fetchone()[0] or 0) + 1

        next_id = (con.execute(
            "SELECT MAX(Id) FROM DrawHistory"
        ).fetchone()[0] or 0) + 1

        con.execute(
            """INSERT INTO DrawHistory
               (Id,LottoType,DrawDate,DrawIndex,Nbr1,Nbr2,Nbr3,Nbr4,Nbr5,Nbr6)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (next_id, lotto_type, draw_date, next_index,
             n1, n2, n3, n4, n5, n6)
        )
        return True


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

def get_lotto_types() -> list:
    with _conn() as con:
        rows = con.execute(
            "SELECT DISTINCT LottoType FROM DrawHistory ORDER BY LottoType"
        ).fetchall()
    return [r["LottoType"] for r in rows]


def get_date_bounds(lotto_type: str) -> tuple:
    """Return (earliest_draw_date, latest_draw_date) as 'YYYY-MM-DD'."""
    with _conn() as con:
        row = con.execute(
            "SELECT MIN(DrawDate) AS lo, MAX(DrawDate) AS hi FROM DrawHistory WHERE LottoType=?",
            (lotto_type,)
        ).fetchone()
    return (row["lo"] or "", row["hi"] or "")


def get_draws_in_window(lotto_type: str, cutoff_past: str, cutoff_now: str) -> list:
    """
    Draws where cutoff_past < DrawDate <= cutoff_now.
    Strict lower bound, inclusive upper — matches legacy dtCutOffPast / dtCutOffNow.
    """
    with _conn() as con:
        rows = con.execute(
            """SELECT DrawIndex,DrawDate,Nbr1,Nbr2,Nbr3,Nbr4,Nbr5,Nbr6
               FROM DrawHistory
               WHERE LottoType=? AND DrawDate>? AND DrawDate<=?
               ORDER BY DrawDate""",
            (lotto_type, cutoff_past, cutoff_now),
        ).fetchall()
    return [dict(r) for r in rows]


def get_draw_by_index(lotto_type: str, draw_index: int):
    with _conn() as con:
        row = con.execute(
            """SELECT DrawIndex,DrawDate,Nbr1,Nbr2,Nbr3,Nbr4,Nbr5,Nbr6
               FROM DrawHistory WHERE LottoType=? AND DrawIndex=?""",
            (lotto_type, draw_index),
        ).fetchone()
    return dict(row) if row else None


def get_draw_by_date(lotto_type: str, draw_date: str):
    with _conn() as con:
        row = con.execute(
            """SELECT DrawIndex,DrawDate,Nbr1,Nbr2,Nbr3,Nbr4,Nbr5,Nbr6
               FROM DrawHistory WHERE LottoType=? AND DrawDate=?""",
            (lotto_type, draw_date),
        ).fetchone()
    return dict(row) if row else None


def get_index_range(lotto_type: str) -> tuple:
    with _conn() as con:
        row = con.execute(
            "SELECT MIN(DrawIndex) AS lo, MAX(DrawIndex) AS hi FROM DrawHistory WHERE LottoType=?",
            (lotto_type,)
        ).fetchone()
    return (row["lo"] or 1, row["hi"] or 1)


def get_date_for_index(lotto_type: str, draw_index: int):
    with _conn() as con:
        row = con.execute(
            "SELECT DrawDate FROM DrawHistory WHERE LottoType=? AND DrawIndex=?",
            (lotto_type, draw_index),
        ).fetchone()
    return row["DrawDate"] if row else None


def get_index_for_date(lotto_type: str, target_date: str) -> int:
    """DrawIndex of draw on or nearest before target_date."""
    with _conn() as con:
        row = con.execute(
            """SELECT DrawIndex FROM DrawHistory
               WHERE LottoType=? AND DrawDate<=?
               ORDER BY DrawDate DESC LIMIT 1""",
            (lotto_type, target_date),
        ).fetchone()
    if row:
        return row["DrawIndex"]
    with _conn() as con:
        row = con.execute(
            "SELECT MIN(DrawIndex) FROM DrawHistory WHERE LottoType=?",
            (lotto_type,)
        ).fetchone()
    return row[0] or 1


def get_existing_dates(lotto_type: str) -> set:
    with _conn() as con:
        rows = con.execute(
            "SELECT DrawDate FROM DrawHistory WHERE LottoType=?",
            (lotto_type,)
        ).fetchall()
    return {r["DrawDate"] for r in rows}


def get_all_draws(lotto_type: str) -> list:
    """
    Return all draws for lotto_type sorted by DrawIndex ASC.
    Each row: DrawIndex, DrawDate, Nbr1..Nbr6.
    Used by gap_engine.find_matches().
    """
    with _conn() as con:
        rows = con.execute(
            """SELECT DrawIndex, DrawDate, Nbr1, Nbr2, Nbr3, Nbr4, Nbr5, Nbr6
               FROM DrawHistory
               WHERE LottoType = ?
               ORDER BY DrawIndex""",
            (lotto_type,),
        ).fetchall()
    return [dict(r) for r in rows]
