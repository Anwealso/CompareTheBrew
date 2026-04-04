#!/usr/bin/env python3
"""Migrate the legacy `efficiency` column to `score` (price per standard drink)."""

from pathlib import Path
import sqlite3

DB_PATH = Path(__file__).resolve().parents[1] / "db" / "database.db"


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """Return True if the given column exists on the table."""
    cursor = conn.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cursor.fetchall())


def rename_column(conn: sqlite3.Connection) -> bool:
    """Rename `efficiency` to `score`, if needed."""
    if not column_exists(conn, "drinks", "efficiency"):
        return False

    conn.execute("ALTER TABLE drinks RENAME COLUMN efficiency TO score")
    conn.commit()
    return True


def recalc_scores(conn: sqlite3.Connection) -> int:
    """Recalculate price per standard drink for all rows."""
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE drinks
        SET score = CASE
            WHEN price > 0 AND stdDrinks > 0 THEN price / stdDrinks
            ELSE NULL
        END
        """
    )
    conn.commit()
    return cursor.rowcount


def recreate_score_index(conn: sqlite3.Connection) -> None:
    """Ensure the new index exists and the old one is removed."""
    cursor = conn.cursor()
    cursor.execute("DROP INDEX IF EXISTS idx_drinks_efficiency")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_drinks_score ON drinks(score)")
    conn.commit()


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        renamed = rename_column(conn)
        if renamed:
            print("Renamed `efficiency` column to `score`.")
        else:
            print("No `efficiency` column found; nothing to rename.")

        updated_rows = recalc_scores(conn)
        print(f"Recalculated price-per-standard-drink for {updated_rows} rows.")

        recreate_score_index(conn)
        print("Rebuilt score index.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
