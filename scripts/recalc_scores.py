#!/usr/bin/env python3
"""
Recalculate every drink score as:
price / (stdDrinks_per_unit * pack_qty)
"""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "db" / "database.db"


def recalc_scores(conn):
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE drinks
        SET score = CASE
            WHEN price > 0
                 AND COALESCE(stdDrinks, 0) > 0
            THEN price / (stdDrinks * MAX(COALESCE(pack_qty, 1), 1))
            ELSE NULL
        END
        """
    )
    conn.commit()
    return cur.rowcount


def main():
    if not DB_PATH.exists():
        raise SystemExit(f"Database not found at {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    try:
        updated = recalc_scores(conn)
        print(f"Recalculated {updated} scores.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
