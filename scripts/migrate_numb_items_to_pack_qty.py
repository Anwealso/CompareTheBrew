#!/usr/bin/env python3
"""Migrate any legacy `numb_items` values into `pack_qty` and drop the old column."""

from pathlib import Path
import sqlite3

DB_PATH = Path(__file__).resolve().parents[1] / "db" / "database.db"


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """Check whether a column exists on a table."""
    cursor = conn.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cursor.fetchall())


def migrate_values(conn: sqlite3.Connection) -> int:
    """Copy `numb_items` into `pack_qty` where pack_qty is still the default."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(1)
        FROM drinks
        WHERE IFNULL(numb_items, 1) != 1
          AND IFNULL(pack_qty, 1) = 1
        """
    )
    count = cursor.fetchone()[0]
    if count == 0:
        return 0

    cursor.execute(
        """
        UPDATE drinks
        SET pack_qty = numb_items
        WHERE IFNULL(numb_items, 1) != 1
          AND IFNULL(pack_qty, 1) = 1
        """
    )
    conn.commit()
    return cursor.rowcount


def recreate_table_without_numb_items(conn: sqlite3.Connection) -> None:
    """Rebuild the drinks table without the deprecated column."""
    cursor = conn.cursor()
    cursor.executescript(
        """
        PRAGMA foreign_keys=off;
        BEGIN TRANSACTION;
        CREATE TABLE drinks_new (
            "ID" INTEGER PRIMARY KEY AUTOINCREMENT,
            "store" TEXT,
            "brand" BLOB,
            "name" NUMERIC,
            "type" TEXT,
            "price" REAL,
            "link" TEXT,
            "pack_qty" INTEGER DEFAULT 1,
            "ml" REAL,
            "percent" REAL,
            "stdDrinks" REAL,
            "score" REAL,
            "image" TEXT,
            "shortimage" TEXT,
            "search_text" TEXT,
            "location" TEXT,
            "date_created" TEXT DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO drinks_new (
            ID, store, brand, name, type, price, link, pack_qty, ml, percent,
            stdDrinks, score, image, shortimage, search_text, location, date_created
        )
        SELECT
            ID, store, brand, name, type, price, link, pack_qty, ml, percent,
            stdDrinks, score, image, shortimage, search_text, location, date_created
        FROM drinks;
        DROP TABLE drinks;
        ALTER TABLE drinks_new RENAME TO drinks;
        COMMIT;
        PRAGMA foreign_keys=on;
        """
    )

    # Recreate indexes that were dropped alongside the table.
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_drinks_search_text ON drinks(search_text);",
        "CREATE INDEX IF NOT EXISTS idx_drinks_store ON drinks(store);",
        "CREATE INDEX IF NOT EXISTS idx_drinks_type ON drinks(type);",
            "CREATE INDEX IF NOT EXISTS idx_drinks_score ON drinks(score);",
        "CREATE INDEX IF NOT EXISTS idx_drinks_price ON drinks(price);",
        "CREATE INDEX IF NOT EXISTS idx_drinks_ml ON drinks(ml);",
        "CREATE INDEX IF NOT EXISTS idx_drinks_store_link_pack_qty ON drinks(store, link, pack_qty);",
    ]
    for stmt in index_statements:
        cursor.execute(stmt)
    conn.commit()


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        if not column_exists(conn, "drinks", "numb_items"):
            print("numb_items column not found; nothing to migrate.")
            return

        updated_count = migrate_values(conn)
        if updated_count:
            print(f"Updated {updated_count} rows by copying numb_items -> pack_qty.")
        else:
            print("No rows required migration; pack_qty already populated.")

        recreate_table_without_numb_items(conn)
        print("Rebuilt drinks table without the numb_items column.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
