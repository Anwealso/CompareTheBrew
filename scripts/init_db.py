#!/usr/bin/env python3
"""
Initialize a new CompareTheBrew database from scratch.
Creates all tables, indexes using schema.sql.
"""
import sqlite3
import sys
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent.parent / "db" / "database.db"
SCHEMA_PATH = Path(__file__).parent.parent / "db" / "schema" / "schema.sql"


def init_db(db_path=None, schema_path=None):
    """Initialize a new database from schema.sql"""
    if db_path:
        path = Path(db_path)
    else:
        path = DB_PATH
    
    if schema_path:
        schema = Path(schema_path)
    else:
        schema = SCHEMA_PATH
    
    # Confirm if file exists
    if path.exists():
        response = input(f"Database already exists at {path}. Delete and recreate? [y/N]: ")
        if response.lower() != 'y':
            print("Aborted.")
            return
        path.unlink()
        print(f"Deleted existing database")
    
    if not schema.exists():
        print(f"Error: Schema file not found at {schema}")
        return
    
    print(f"\nInitializing new database at {path}")
    print(f"Using schema: {schema}")
    print("-" * 40)
    
    conn = sqlite3.connect(path)
    
    try:
        # Execute each table SQL file
        tables_dir = schema.parent / "tables"
        for table_file in sorted(tables_dir.glob("*.sql")):
            print(f"  Applying {table_file.name}...")
            conn.executescript(table_file.read_text())
        
        conn.commit()
        
        # Set initial schema version
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
            (14, datetime.now().isoformat())
        )
        conn.commit()
        
        print("Created all tables and indexes")
        print("-" * 40)
        print("Database initialized successfully!")
        print(f"Schema version: 1.4")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else None
    schema_path = sys.argv[2] if len(sys.argv) > 2 else None
    init_db(db_path, schema_path)
