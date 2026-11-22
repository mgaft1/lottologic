from __future__ import annotations

import sqlite3
from datetime import date, timedelta

import db_ticket_sim


def _use_temp_db(tmp_path):
    db_ticket_sim.DB_PATH = str(tmp_path / "tickets.db")
    db_ticket_sim.init_ticket_schema()


def test_legacy_purchased_values_are_migrated(tmp_path):
    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(db_path) as con:
        con.executescript(
            """
            CREATE TABLE TicketSimSelections (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                LottoType CHAR(2) NOT NULL,
                DrawDate DATE NOT NULL,
                Purchased INTEGER NOT NULL DEFAULT 0,
                TicketPrice REAL NOT NULL DEFAULT 0,
                Nbr1 INTEGER NOT NULL, Nbr2 INTEGER NOT NULL, Nbr3 INTEGER NOT NULL,
                Nbr4 INTEGER NOT NULL, Nbr5 INTEGER NOT NULL, Nbr6 INTEGER,
                CreatedAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO TicketSimSelections
                (LottoType, DrawDate, Purchased, TicketPrice, Nbr1, Nbr2, Nbr3, Nbr4, Nbr5, Nbr6)
            VALUES
                ('CA', '2026-11-21', 0, 1, 1, 2, 3, 4, 5, 6),
                ('CA', '2026-11-21', 1, 1, 7, 8, 9, 10, 11, 12);
            """
        )

    db_ticket_sim.DB_PATH = str(db_path)
    db_ticket_sim.init_ticket_schema()
    rows = db_ticket_sim.get_saved_tickets()
    assert {row["Status"] for row in rows} == {"OPTIONAL", "PURCHASED"}


def test_status_can_move_in_both_directions(tmp_path):
    _use_temp_db(tmp_path)
    ticket_id = db_ticket_sim.add_ticket("CA", "2026-11-21", 2, [1, 2, 3, 4, 5, 6], status="OPTIONAL")

    for status, purchased in (("PLANNED", 0), ("PURCHASED", 1), ("OPTIONAL", 0)):
        assert db_ticket_sim.update_ticket_status(ticket_id, status)
        row = db_ticket_sim.get_saved_tickets()[0]
        assert row["Status"] == status
        assert row["Purchased"] == purchased


def test_only_old_optional_tickets_expire(tmp_path):
    _use_temp_db(tmp_path)
    old_draw = (date.today() - timedelta(days=30)).isoformat()
    # Use today rather than a boundary date because SQLite DATE('now') is UTC.
    recent_draw = date.today().isoformat()
    db_ticket_sim.add_ticket("CA", old_draw, 1, [1, 2, 3, 4, 5, 6], status="OPTIONAL")
    db_ticket_sim.add_ticket("CA", old_draw, 2, [7, 8, 9, 10, 11, 12], status="PLANNED")
    db_ticket_sim.add_ticket("CA", old_draw, 3, [13, 14, 15, 16, 17, 18], status="PURCHASED")
    db_ticket_sim.add_ticket("CA", recent_draw, 4, [19, 20, 21, 22, 23, 24], status="OPTIONAL")

    assert db_ticket_sim.purge_expired_tickets() == 1
    rows = db_ticket_sim.get_saved_tickets()
    assert {row["Status"] for row in rows} == {"OPTIONAL", "PLANNED", "PURCHASED"}
    assert db_ticket_sim.get_total_spent("CA", old_draw) == 3


def test_invalid_status_is_rejected(tmp_path):
    _use_temp_db(tmp_path)
    try:
        db_ticket_sim.add_ticket("CA", "2026-11-21", 1, [1, 2, 3, 4, 5, 6], status="maybe")
    except ValueError:
        pass
    else:
        raise AssertionError("Invalid ticket status was accepted")
