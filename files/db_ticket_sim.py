from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path


DB_PATH = os.environ.get("LOTTO_DB", str(Path(__file__).parent.parent / "data" / "lotto.db"))


@contextmanager
def _conn():
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


def init_ticket_schema() -> None:
    with _conn() as con:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS TicketSimSelections (
                Id          INTEGER PRIMARY KEY AUTOINCREMENT,
                LottoType   CHAR(2)      NOT NULL,
                DrawDate    DATE         NOT NULL,
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


def purge_expired_tickets() -> int:
    with _conn() as con:
        cur = con.execute(
            """
            DELETE FROM TicketSimSelections
            WHERE DATE(DrawDate, '+7 day') < DATE('now')
            """
        )
        return int(cur.rowcount or 0)


def add_ticket(lotto_type: str, draw_date: str, price: float, numbers: list[int]) -> int:
    with _conn() as con:
        cur = con.execute(
            """
            INSERT INTO TicketSimSelections
            (LottoType, DrawDate, TicketPrice, Nbr1, Nbr2, Nbr3, Nbr4, Nbr5, Nbr6)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lotto_type,
                draw_date,
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


def delete_ticket(ticket_id: int) -> None:
    with _conn() as con:
        con.execute("DELETE FROM TicketSimSelections WHERE Id = ?", (ticket_id,))


def get_tickets(lotto_type: str, draw_date: str) -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            """
            SELECT Id, LottoType, DrawDate, TicketPrice, Nbr1, Nbr2, Nbr3, Nbr4, Nbr5, Nbr6, CreatedAt
            FROM TicketSimSelections
            WHERE LottoType = ? AND DrawDate = ?
            ORDER BY Id
            """,
            (lotto_type, draw_date),
        ).fetchall()
    return [dict(r) for r in rows]


def get_total_spent(lotto_type: str, draw_date: str) -> float:
    with _conn() as con:
        row = con.execute(
            """
            SELECT COALESCE(SUM(TicketPrice), 0) AS total_spent
            FROM TicketSimSelections
            WHERE LottoType = ? AND DrawDate = ?
            """,
            (lotto_type, draw_date),
        ).fetchone()
    return float(row["total_spent"] or 0)
