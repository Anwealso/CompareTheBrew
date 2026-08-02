"""
Shared fixtures for the scraping test suite.

Design (see scraping/TESTING_DESIGN.md):
- `temp_db` gives each test an isolated throwaway SQLite DB with the full
  schema, via the SQLITE_DB_PATH override in db/databaseBackend.py. Nothing
  touches the real dev database.
- `fake_fetchers` installs offline fetchers so the real controller,
  processors and DB run end-to-end against checked-in fixtures with zero
  network and zero ScrapingBee credits. It patches every fetch entry point:
  the shared `Fetcher._implementation` singleton (BWS + base) and the
  Liquorland processor's own `fetch_url` / `fetch_url_max_rpp` (which bypass
  that singleton in production).

Plain helpers (FakeProcessor, load_fixture, default_resolver) live in
tests/scraping/_support.py so they can be imported directly.
"""
import pytest

from tests.scraping._support import FakeImpl, default_resolver


@pytest.fixture
def temp_db(tmp_path, monkeypatch):
    """Yield a connection to a fresh, schema-loaded, throwaway SQLite DB."""
    from config import Config

    db_file = tmp_path / "test.db"
    monkeypatch.setenv("SQLITE_DB_PATH", str(db_file))
    monkeypatch.setattr(Config, "USE_LOCAL_DB", True, raising=False)

    import db.databaseHandler as dbh

    conn = dbh.create_connection()
    dbh.ensure_tables(conn)
    conn.commit()
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def fake_fetchers(monkeypatch):
    """
    Return an installer: `fake_fetchers(resolver=default_resolver)` patches
    every fetch entry point to replay fixtures, and returns a call log.
    """
    def _install(resolver=default_resolver):
        import scraping.fetcher as fetcher_mod
        from scraping.liquorland_processor import LiquorlandProcessor

        impl = FakeImpl(resolver)
        # BWS + base processors go through the shared singleton impl.
        monkeypatch.setattr(fetcher_mod.Fetcher, "_implementation", impl)
        # Liquorland bypasses the singleton with its own fetch methods.
        monkeypatch.setattr(
            LiquorlandProcessor, "fetch_url",
            lambda self, url: resolver(url), raising=True,
        )
        monkeypatch.setattr(
            LiquorlandProcessor, "fetch_url_max_rpp",
            lambda self, url: resolver(url), raising=True,
        )
        return impl.calls

    return _install
