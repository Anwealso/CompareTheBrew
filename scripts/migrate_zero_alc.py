#!/usr/bin/env python3
"""
Ensure every drink row has the zero_alc flag and it reflects whether the ABV is <= 0.5%.
"""
import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent.parent / "db" / "database.db"


def column_exists(cursor, table, column):
    cursor.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cursor.fetchall())


def ensure_zero_alc_column(conn):
    cur = conn.cursor()
    if not column_exists(cur, "drinks", "zero_alc"):
        cur.execute("ALTER TABLE drinks ADD COLUMN zero_alc INTEGER DEFAULT 0")
        conn.commit()


def backfill_zero_alc(conn):
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE drinks
        SET zero_alc = CASE
            WHEN COALESCE(percent, 0.0) <= 0.5 THEN 1
            ELSE 0
        END
        """
    )
    conn.commit()


def stamp_schema_version(conn, version: int):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM schema_version WHERE version = ?", (version,))
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
            (version, datetime.now().isoformat()),
        )
        conn.commit()


def main():
    if not DB_PATH.exists():
        raise SystemExit(f"Database file not found at {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_zero_alc_column(conn)
        backfill_zero_alc(conn)
        stamp_schema_version(conn, 14)
        print("zero_alc migration applied successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
