#!/usr/bin/env python3
"""
Migrate only drink data from local SQLite to Supabase PostgreSQL.

Usage:
    python scripts/migrate_drinks_to_supabase.py

Requirements:
    - ADMIN_SUPABASE_DB_URL set in .env
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import Config
import sqlite3


def get_sqlite_connection():
    db_path = Path(__file__).parent.parent / "db" / "database.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def get_postgres_connection():
    import psycopg
    conn_string = Config.ADMIN_SUPABASE_DB_URL
    if not conn_string:
        raise ValueError("ADMIN_SUPABASE_DB_URL environment variable is not set")
    return psycopg.connect(conn_string)


DRINKS_COLUMNS = (
    "store,brand,name,type,price,link,pack_qty,ml,percent,"
    "stdDrinks,score,image,shortimage,search_text,location,"
    "date_created,zero_alc"
)


def migrate_drinks(sqlite_cur, pg_cur):
    print("Fetching drinks from SQLite...")
    sqlite_cur.execute(f"SELECT {DRINKS_COLUMNS} FROM drinks")
    rows = sqlite_cur.fetchall()

    if not rows:
        print("No drinks found in SQLite.")
        return 0

    col_list = DRINKS_COLUMNS.split(",")
    placeholders = ",".join(["%s"] * len(col_list))
    values = [list(row) for row in rows]

    print(f"Inserting {len(rows)} drinks into Supabase...")
    pg_cur.executemany(
        f"INSERT INTO drinks ({DRINKS_COLUMNS}) VALUES ({placeholders}) "
        f"ON CONFLICT DO NOTHING",
        values,
    )
    print(f"Done. {len(rows)} rows processed.")
    return len(rows)


def main():
    print("=" * 60)
    print("Drinks-only SQLite → Supabase Migration")
    print("=" * 60)

    if not Config.ADMIN_SUPABASE_DB_URL:
        print("\nERROR: ADMIN_SUPABASE_DB_URL not set in .env")
        sys.exit(1)

    print("\nConnecting to SQLite...")
    sqlite_conn = get_sqlite_connection()
    sqlite_cur = sqlite_conn.cursor()

    print("Connecting to Supabase...")
    pg_conn = get_postgres_connection()
    pg_cur = pg_conn.cursor()

    total = migrate_drinks(sqlite_cur, pg_cur)

    pg_conn.commit()
    sqlite_conn.close()
    pg_conn.close()

    print("\n" + "=" * 60)
    print(f"Migration complete! Rows processed: {total}")
    print("=" * 60)


if __name__ == "__main__":
    main()
