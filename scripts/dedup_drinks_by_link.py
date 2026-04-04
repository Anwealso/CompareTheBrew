"""Deduplicate the drinks table by store/link/pack_qty, keeping the best entry."""
import sqlite3
from pathlib import Path

from typing import Optional


def get_db_path() -> Path:
    return Path(__file__).parent.parent / "db" / "database.db"


def qualify_row(row: sqlite3.Row) -> bool:
    """Return True if percent/stdDrinks are both positive."""
    percent = row["percent"]
    std_drinks = row["stdDrinks"]
    try:
        percent_value = float(percent) if percent is not None else 0.0
        std_value = float(std_drinks) if std_drinks is not None else 0.0
    except (TypeError, ValueError):
        return False
    return percent_value > 0 and std_value > 0


def run() -> None:
    db_path = get_db_path()
    if not db_path.exists():
        raise SystemExit(f"database not found at {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT store, link, pack_qty
        FROM drinks
        WHERE link IS NOT NULL AND link != ''
        GROUP BY store, link, pack_qty
        HAVING COUNT(*) > 1
        """
    )

    duplicates = [(row["store"], row["link"], row["pack_qty"]) for row in cursor.fetchall()]
    deleted = 0

    for store, link, pack_qty in duplicates:
        cursor.execute(
            "SELECT * FROM drinks WHERE store = ? AND link = ? AND pack_qty = ? ORDER BY ID DESC",
            (store, link, pack_qty),
        )
        rows = cursor.fetchall()
        if not rows:
            continue

        keep: Optional[sqlite3.Row] = None
        positive_rows = [row for row in rows if qualify_row(row)]
        if positive_rows:
            keep = max(positive_rows, key=lambda r: r["ID"])
        else:
            keep = max(rows, key=lambda r: r["ID"])

        keep_id = keep["ID"]
        ids_to_delete = [row["ID"] for row in rows if row["ID"] != keep_id]
        if not ids_to_delete:
            continue

        cursor.executemany("DELETE FROM drinks WHERE ID = ?", ((row_id,) for row_id in ids_to_delete))
        deleted += len(ids_to_delete)

    conn.commit()
    conn.close()

    print(f"Removed {deleted} duplicate drinks and kept {deleted + len(duplicates)} unique store/link/pack_qty groups.")


if __name__ == "__main__":
    run()
