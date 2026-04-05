"""
Database backend module that handles connection to either SQLite (local) or PostgreSQL (Supabase).
"""

import os
import sqlite3
from pathlib import Path
from config import Config


def _get_db_path():
    return Path(__file__).parent / "database.db"


def _create_sqlite_connection():
    """Create a SQLite connection for local development."""
    db_path = _get_db_path()
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _create_postgres_connection():
    """Create a PostgreSQL connection to Supabase."""
    import psycopg2
    conn_string = Config.SUPABASE_DB_URL
    if not conn_string:
        raise ValueError("SUPABASE_DB_URL environment variable is not set")
    conn = psycopg2.connect(conn_string)
    return conn


def create_connection():
    """
    Create a database connection based on configuration.
    
    If USE_LOCAL_DB is set to 'true', uses SQLite (local development).
    Otherwise, uses PostgreSQL with SUPABASE_DB_URL (production/Supabase).
    """
    if Config.USE_LOCAL_DB:
        return _create_sqlite_connection()
    else:
        return _create_postgres_connection()


def get_cursor(conn):
    """Get a cursor for the given connection."""
    return conn.cursor()


def is_sqlite():
    """Return True if using SQLite (local), False if using PostgreSQL (Supabase)."""
    return Config.USE_LOCAL_DB


def get_backend_name():
    """Return the name of the current backend."""
    return "SQLite (local)" if is_sqlite() else "PostgreSQL (Supabase)"