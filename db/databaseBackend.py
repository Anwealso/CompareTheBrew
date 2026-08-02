"""
Database backend module that handles connection to either SQLite (local) or PostgreSQL (Supabase).
"""

import os
import sqlite3
from pathlib import Path
from config import Config


def _get_db_path():
    # Allow tests (and other tooling) to point at a throwaway SQLite file via
    # SQLITE_DB_PATH so they never touch the real dev database.
    override = os.environ.get("SQLITE_DB_PATH")
    if override:
        return Path(override)
    return Path(__file__).parent / "database.db"


def _create_sqlite_connection():
    """Create a SQLite connection for local development."""
    db_path = _get_db_path()
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _create_postgres_connection():
    """Create a PostgreSQL connection to Supabase."""
    import socket
    import psycopg
    from urllib.parse import urlparse

    conn_string = Config.SUPABASE_DB_URL
    if not conn_string:
        raise ValueError("SUPABASE_DB_URL environment variable is not set")

    # Try connecting directly (IPv6 if the hostname resolves to it), then
    # fall back to an explicit IPv4 address if that fails.
    try:
        return psycopg.connect(conn_string)
    except Exception as original_exc:
        try:
            hostname = urlparse(conn_string).hostname
            ipv4 = socket.getaddrinfo(hostname, None, socket.AF_INET)[0][4][0]
            return psycopg.connect(conn_string, hostaddr=ipv4)
        except (socket.gaierror, IndexError):
            raise original_exc


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
