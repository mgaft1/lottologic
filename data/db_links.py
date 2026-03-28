"""
db_links.py  --  Data access layer for UserLinks table.

Additive — shares the same lotto.db database.
No modifications to any existing tables.
"""

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

def _resolve_db_path() -> str:
    if os.environ.get("LOTTO_DB"):
        return os.environ["LOTTO_DB"]
    if os.environ.get("LOTTO_DB_DIR"):
        return str(Path(os.environ["LOTTO_DB_DIR"]) / "lotto.db")
    if os.environ.get("RENDER_DISK_PATH"):
        return str(Path(os.environ["RENDER_DISK_PATH"]) / "lotto.db")
    return str(Path(__file__).resolve().parent.parent / "data" / "lotto.db")


DB_PATH = _resolve_db_path()

MAX_LINKS = 100

VALID_CATEGORIES = {"music", "cooking", "baking"}


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

def init_links_schema() -> None:
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS UserLinks (
                Id         INTEGER PRIMARY KEY AUTOINCREMENT,
                Category   TEXT NOT NULL,
                Title      TEXT NOT NULL,
                Url        TEXT NOT NULL,
                CreatedAt  TEXT NOT NULL
            )
        """)


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

def get_all_links() -> list[dict]:
    """Return all links ordered by Category, then CreatedAt DESC."""
    with _conn() as con:
        rows = con.execute(
            "SELECT Id, Category, Title, Url, CreatedAt "
            "FROM UserLinks ORDER BY Category, CreatedAt DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def get_link(link_id: int) -> Optional[dict]:
    with _conn() as con:
        row = con.execute(
            "SELECT Id, Category, Title, Url, CreatedAt FROM UserLinks WHERE Id = ?",
            (link_id,)
        ).fetchone()
    return dict(row) if row else None


def count_links() -> int:
    with _conn() as con:
        row = con.execute("SELECT COUNT(*) FROM UserLinks").fetchone()
    return row[0]


def add_link(category: str, title: str, url: str) -> int:
    """Insert a new link. Returns new Id."""
    created_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with _conn() as con:
        cur = con.execute(
            "INSERT INTO UserLinks (Category, Title, Url, CreatedAt) VALUES (?, ?, ?, ?)",
            (category.lower(), title, url, created_at)
        )
    return cur.lastrowid


def update_link(link_id: int, category: str, title: str, url: str) -> bool:
    """Update an existing link. Returns True if a row was updated."""
    with _conn() as con:
        cur = con.execute(
            "UPDATE UserLinks SET Category=?, Title=?, Url=? WHERE Id=?",
            (category.lower(), title, url, link_id)
        )
    return cur.rowcount > 0


def delete_link(link_id: int) -> bool:
    """Delete a link. Returns True if a row was deleted."""
    with _conn() as con:
        cur = con.execute("DELETE FROM UserLinks WHERE Id=?", (link_id,))
    return cur.rowcount > 0
