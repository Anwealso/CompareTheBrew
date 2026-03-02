#!/usr/bin/env python3
"""
Schema management script for CompareTheBrew.
Keeps schema.sql in sync with the actual database.
"""
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "database.db"
SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def export_schema():
    """Export current database schema to schema.sql"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all table schemas
    cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = [row[0] for row in cursor.fetchall() if row[0]]
    
    # Get all indexes
    cursor.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'")
    indexes = [row[0] for row in cursor.fetchall() if row[0]]
    
    conn.close()
    
    # Write schema.sql
    with open(SCHEMA_PATH, 'w') as f:
        f.write("-- Auto-generated schema file - DO NOT EDIT MANUALLY\n")
        f.write("-- Run 'python3 scripts/sync_schema.py' to update\n\n")
        
        for table in tables:
            f.write(table + ";\n\n")
        
        for index in indexes:
            f.write(index + ";\n\n")
    
    print(f"Exported schema to {SCHEMA_PATH}")
    print(f"  - {len(tables)} tables")
    print(f"  - {len(indexes)} indexes")


def get_current_version():
    """Get current schema version from database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'")
    if not cursor.fetchone():
        conn.close()
        return None
    
    cursor.execute("SELECT version FROM schema_version ORDER BY id DESC LIMIT 1")
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None


def init_version():
    """Initialize schema version table"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version INTEGER NOT NULL,
            applied_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("INSERT INTO schema_version (version) VALUES (1)")
    conn.commit()
    conn.close()
    print("Initialized schema_version table at v1")


def main():
    if len(sys.argv) > 1:
        if sys.argv[1] == "export":
            export_schema()
        elif sys.argv[1] == "version":
            v = get_current_version()
            print(f"Schema version: {v or 'not set'}")
        elif sys.argv[1] == "init":
            init_version()
        else:
            print(f"Unknown command: {sys.argv[1]}")
            print("Usage: python3 sync_schema.py [export|version|init]")
    else:
        # Default: export current schema
        export_schema()


if __name__ == "__main__":
    main()
