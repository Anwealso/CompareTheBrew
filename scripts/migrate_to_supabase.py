#!/usr/bin/env python3
"""
Database migration script - migrate SQLite data to Supabase PostgreSQL.

Usage:
    python scripts/migrate_to_supabase.py

Requirements:
    - USE_LOCAL_DB=false in .env (or set it)
    - SUPABASE_DB_URL set with your connection string
"""

import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import Config
import sqlite3


def get_sqlite_connection():
    """Connect to local SQLite database."""
    db_path = Path(__file__).parent.parent / "db" / "database.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def get_postgres_connection():
    """Connect to Supabase PostgreSQL database."""
    import psycopg
    conn_string = Config.SUPABASE_DB_URL
    if not conn_string:
        raise ValueError("SUPABASE_DB_URL environment variable is not set")
    return psycopg.connect(conn_string)


def create_tables_pg(cur):
    """Create tables in PostgreSQL with compatible schemas."""
    
    # Drinks table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS drinks (
            ID SERIAL PRIMARY KEY,
            store TEXT,
            brand TEXT,
            name TEXT,
            type TEXT,
            price REAL,
            link TEXT,
            pack_qty INTEGER DEFAULT 1,
            ml REAL,
            percent REAL,
            stdDrinks REAL,
            score REAL,
            image TEXT,
            shortimage TEXT,
            search_text TEXT,
            location TEXT,
            date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            zero_alc INTEGER DEFAULT 0
        )
    """)
    
    # Indexes for drinks
    cur.execute("CREATE INDEX IF NOT EXISTS idx_drinks_search_text ON drinks(search_text)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_drinks_store ON drinks(store)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_drinks_type ON drinks(type)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_drinks_score ON drinks(score)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_drinks_price ON drinks(price)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_drinks_ml ON drinks(ml)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_drinks_store_link_pack_qty ON drinks(store, link, pack_qty)")
    
    # Metrics table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            ID SERIAL PRIMARY KEY,
            metric_name TEXT NOT NULL,
            key TEXT,
            value INTEGER DEFAULT 0,
            UNIQUE(metric_name, key)
        )
    """)
    
    # Sources table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            ID SERIAL PRIMARY KEY,
            url TEXT UNIQUE NOT NULL,
            retailer TEXT,
            last_scraped TIMESTAMP
        )
    """)
    
    # Schema version table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            version_no INTEGER,
            updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Scrape tasks table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scrape_tasks (
            ID SERIAL PRIMARY KEY,
            retailer TEXT,
            url TEXT,
            status TEXT DEFAULT 'pending',
            task_type TEXT,
            metadata TEXT,
            run_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            attempts INTEGER DEFAULT 0
        )
    """)
    
    # Runs table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            ID SERIAL PRIMARY KEY,
            uuid TEXT UNIQUE,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            status TEXT,
            retailer TEXT,
            category TEXT,
            tasks_completed INTEGER DEFAULT 0
        )
    """)
    
    # Request logs table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS request_logs (
            ID SERIAL PRIMARY KEY,
            ip TEXT,
            query TEXT,
            datetime TIMESTAMP,
            country TEXT,
            region TEXT,
            city TEXT,
            lat REAL,
            long REAL,
            hostname TEXT,
            org TEXT
        )
    """)
    
    print("Tables created in PostgreSQL")


def migrate_table(sqlite_cur, pg_cur, table_name, columns):
    """Migrate a single table from SQLite to PostgreSQL."""
    print(f"Migrating {table_name}...")
    
    # Get all rows from SQLite
    sqlite_cur.execute(f"SELECT {columns} FROM {table_name}")
    rows = sqlite_cur.fetchall()
    
    if not rows:
        print(f"  No data to migrate for {table_name}")
        return 0
    
    # Get placeholders for values
    placeholders = ",".join(["%s"] * len(columns.split(",")))

    # Bulk insert into PostgreSQL
    values = [list(row) for row in rows]
    pg_cur.executemany(
        f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})",
        values
    )
    
    count = len(rows)
    print(f"  Migrated {count} rows from {table_name}")
    return count


def main():
    print("=" * 60)
    print("SQLite to Supabase Migration Script")
    print("=" * 60)
    
    # Check config
    print(f"\nUSE_LOCAL_DB: {Config.USE_LOCAL_DB}")
    print(f"SUPABASE_DB_URL set: {bool(Config.SUPABASE_DB_URL)}")
    
    if not Config.SUPABASE_DB_URL:
        print("\nERROR: SUPABASE_DB_URL not set in .env")
        print("Please set it and try again.")
        sys.exit(1)
    
    # Connect to both databases
    print("\nConnecting to SQLite...")
    sqlite_conn = get_sqlite_connection()
    sqlite_cur = sqlite_conn.cursor()
    
    print("Connecting to Supabase...")
    pg_conn = get_postgres_connection()
    pg_cur = pg_conn.cursor()
    
    # Create tables
    print("\nCreating tables in Supabase...")
    create_tables_pg(pg_cur)
    pg_conn.commit()
    
    # Migrate data
    total = 0
    
    # Drinks table
    columns = "store,brand,name,type,price,link,pack_qty,ml,percent,stdDrinks,score,image,shortimage,search_text,location,date_created,zero_alc"
    total += migrate_table(sqlite_cur, pg_cur, "drinks", columns)
    
    # Metrics table
    columns = "metric_name,key,value"
    total += migrate_table(sqlite_cur, pg_cur, "metrics", columns)
    
    # Sources table
    columns = "url,retailer,last_scraped"
    total += migrate_table(sqlite_cur, pg_cur, "sources", columns)
    
    # Scrape tasks table
    columns = "retailer,url,status,task_type,metadata,run_id,created_at,updated_at,attempts"
    total += migrate_table(sqlite_cur, pg_cur, "scrape_tasks", columns)
    
    # Runs table
    columns = "uuid,start_time,end_time,status,retailer,category,tasks_completed"
    total += migrate_table(sqlite_cur, pg_cur, "runs", columns)
    
    # Request logs table
    columns = "ip,query,datetime,country,region,city,lat,long,hostname,org"
    total += migrate_table(sqlite_cur, pg_cur, "request_logs", columns)
    
    # Commit and close
    pg_conn.commit()
    sqlite_conn.close()
    pg_conn.close()
    
    print("\n" + "=" * 60)
    print(f"Migration complete! Total rows migrated: {total}")
    print("=" * 60)


if __name__ == "__main__":
    main()