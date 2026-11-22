from __future__ import annotations

import os
import sqlite3
import time
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
SQLITE_TIMEOUT_SECS = float(os.environ.get("LOTTO_SQLITE_TIMEOUT_SECS", "30"))
TICKET_STATUSES = ("OPTIONAL", "PLANNED", "PURCHASED")


def normalize_status(status: str | None) -> str:
    value = str(status or "OPTIONAL").strip().upper()
    if value not in TICKET_STATUSES:
        raise ValueError(f"Invalid ticket status: {status}")
    return value

def _journal_mode() -> str:
    mode = os.environ.get("LOTTO_SQLITE_JOURNAL_MODE", "DELETE").upper()
    return mode if mode in {"DELETE", "WAL", "TRUNCATE", "PERSIST", "MEMORY", "OFF"} else "DELETE"


@contextmanager
def _conn():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=SQLITE_TIMEOUT_SECS)
    con.row_factory = sqlite3.Row
    con.execute(f"PRAGMA busy_timeout={int(SQLITE_TIMEOUT_SECS * 1000)}")
    con.execute("PRAGMA foreign_keys=ON")
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def init_ticket_schema() -> None:
    with _conn() as con:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS TicketSimSelections (
                Id          INTEGER PRIMARY KEY AUTOINCREMENT,
                LottoType   CHAR(2)      NOT NULL,
                DrawDate    DATE         NOT NULL,
                Purchased   INTEGER      NOT NULL DEFAULT 0,
                Status      TEXT         NOT NULL DEFAULT 'OPTIONAL',
                TicketPrice REAL         NOT NULL DEFAULT 0,
                Nbr1        INTEGER      NOT NULL,
                Nbr2        INTEGER      NOT NULL,
                Nbr3        INTEGER      NOT NULL,
                Nbr4        INTEGER      NOT NULL,
                Nbr5        INTEGER      NOT NULL,
                Nbr6        INTEGER      NULL,
                CreatedAt   TEXT         NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS IX_TicketSimSelections_Type_Date
                ON TicketSimSelections (LottoType, DrawDate);
            """
        )
        cols = {row["name"] for row in con.execute("PRAGMA table_info(TicketSimSelections)").fetchall()}
        if "Purchased" not in cols:
            con.execute("ALTER TABLE TicketSimSelections ADD COLUMN Purchased INTEGER NOT NULL DEFAULT 0")
        if "Status" not in cols:
            con.execute("ALTER TABLE TicketSimSelections ADD COLUMN Status TEXT NOT NULL DEFAULT 'OPTIONAL'")
            con.execute(
                """
                UPDATE TicketSimSelections
                SET Status = CASE WHEN Purchased = 1 THEN 'PURCHASED' ELSE 'OPTIONAL' END
                """
            )
        con.execute(
            """
            UPDATE TicketSimSelections
            SET Status = CASE WHEN Purchased = 1 THEN 'PURCHASED' ELSE 'OPTIONAL' END
            WHERE Status IS NULL OR UPPER(Status) NOT IN ('OPTIONAL', 'PLANNED', 'PURCHASED')
            """
        )
        con.execute(
            """
            UPDATE TicketSimSelections
            SET Purchased = CASE WHEN UPPER(Status) = 'PURCHASED' THEN 1 ELSE 0 END
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS IX_TicketSimSelections_Status_Date
            ON TicketSimSelections (Status, DrawDate)
            """
        )


def purge_expired_tickets() -> int:
    try:
        with _conn() as con:
            cur = con.execute(
                """
                DELETE FROM TicketSimSelections
                WHERE UPPER(Status) = 'OPTIONAL'
                  AND DATE(DrawDate, '+14 day') < DATE('now')
                """
            )
            return int(cur.rowcount or 0)
    except sqlite3.OperationalError as exc:
        if "locked" in str(exc).lower():
            return 0
        raise


def ticket_exists(lotto_type: str, draw_date: str, numbers: list[int]) -> bool:
    with _conn() as con:
        row = con.execute(
            """
            SELECT 1
            FROM TicketSimSelections
            WHERE LottoType = ?
              AND DrawDate = ?
              AND Nbr1 = ?
              AND Nbr2 = ?
              AND Nbr3 = ?
              AND Nbr4 = ?
              AND Nbr5 = ?
              AND COALESCE(Nbr6, -1) = COALESCE(?, -1)
            LIMIT 1
            """,
            (
                lotto_type,
                draw_date,
                numbers[0],
                numbers[1],
                numbers[2],
                numbers[3],
                numbers[4],
                numbers[5] if len(numbers) > 5 else None,
            ),
        ).fetchone()
    return row is not None


def add_ticket(
    lotto_type: str,
    draw_date: str,
    price: float,
    numbers: list[int],
    status: str = "OPTIONAL",
) -> int | None:
    status = normalize_status(status)
    for attempt in range(5):
        try:
            if ticket_exists(lotto_type, draw_date, numbers):
                return None
            with _conn() as con:
                cur = con.execute(
                    """
                    INSERT INTO TicketSimSelections
                    (LottoType, DrawDate, Purchased, Status, TicketPrice, Nbr1, Nbr2, Nbr3, Nbr4, Nbr5, Nbr6)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        lotto_type,
                        draw_date,
                        1 if status == "PURCHASED" else 0,
                        status,
                        price,
                        numbers[0],
                        numbers[1],
                        numbers[2],
                        numbers[3],
                        numbers[4],
                        numbers[5] if len(numbers) > 5 else None,
                    ),
                )
                return int(cur.lastrowid)
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt == 4:
                raise
            time.sleep(0.2 * (attempt + 1))
    return None


def delete_ticket(ticket_id: int) -> bool:
    for attempt in range(3):
        try:
            with _conn() as con:
                cur = con.execute("DELETE FROM TicketSimSelections WHERE Id = ?", (ticket_id,))
                return int(cur.rowcount or 0) > 0
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt == 2:
                raise
            time.sleep(0.2 * (attempt + 1))
    return False


def delete_optional_tickets(lotto_type: str, draw_date: str) -> int:
    for attempt in range(3):
        try:
            with _conn() as con:
                cur = con.execute(
                    """
                    DELETE FROM TicketSimSelections
                    WHERE LottoType = ? AND DrawDate = ? AND UPPER(Status) = 'OPTIONAL'
                    """,
                    (lotto_type, draw_date),
                )
                return int(cur.rowcount or 0)
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt == 2:
                raise
            time.sleep(0.2 * (attempt + 1))
    return 0


def update_ticket_status(ticket_id: int, status: str) -> bool:
    status = normalize_status(status)
    for attempt in range(5):
        try:
            with _conn() as con:
                cur = con.execute(
                    "UPDATE TicketSimSelections SET Status = ?, Purchased = ? WHERE Id = ?",
                    (status, 1 if status == "PURCHASED" else 0, ticket_id),
                )
                return int(cur.rowcount or 0) > 0
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt == 4:
                raise
            time.sleep(0.2 * (attempt + 1))
    return False


def get_tickets(lotto_type: str, draw_date: str) -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            """
            SELECT Id, LottoType, DrawDate, Purchased, Status, TicketPrice, Nbr1, Nbr2, Nbr3, Nbr4, Nbr5, Nbr6, CreatedAt
            FROM TicketSimSelections
            WHERE LottoType = ? AND DrawDate = ?
            ORDER BY Nbr1, Nbr2, Nbr3, Nbr4, Nbr5, COALESCE(Nbr6, -1), Id
            """,
            (lotto_type, draw_date),
        ).fetchall()
    return [dict(r) for r in rows]


def get_saved_tickets(
    lotto_type: str | None = None,
    status: str | None = None,
    draw_date: str | None = None,
) -> list[dict]:
    clauses = []
    params: list[object] = []
    if lotto_type:
        clauses.append("LottoType = ?")
        params.append(lotto_type)
    if status:
        clauses.append("UPPER(Status) = ?")
        params.append(normalize_status(status))
    if draw_date:
        clauses.append("DrawDate = ?")
        params.append(draw_date)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _conn() as con:
        rows = con.execute(
            f"""
            SELECT Id, LottoType, DrawDate, Purchased, Status, TicketPrice,
                   Nbr1, Nbr2, Nbr3, Nbr4, Nbr5, Nbr6, CreatedAt
            FROM TicketSimSelections
            {where}
            ORDER BY DrawDate DESC, LottoType, Id DESC
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def get_total_spent(lotto_type: str, draw_date: str) -> float:
    with _conn() as con:
        row = con.execute(
            """
            SELECT COALESCE(SUM(TicketPrice), 0) AS total_spent
            FROM TicketSimSelections
            WHERE LottoType = ? AND DrawDate = ? AND UPPER(Status) = 'PURCHASED'
            """,
            (lotto_type, draw_date),
        ).fetchone()
    return float(row["total_spent"] or 0)
