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
- `FakeProcessor` is a canned RetailerProcessor for offline queue/controller
  tests where the retailer parsing is irrelevant.
"""
import json
import re
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


# --------------------------------------------------------------------------
# Isolated database
# --------------------------------------------------------------------------
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


# --------------------------------------------------------------------------
# Offline fetchers (fixture replay)
# --------------------------------------------------------------------------
def default_resolver(url: str):
    """Map a fetch URL to the fixture that should stand in for it."""
    u = url.lower()
    if "bws" in u:
        return load_fixture("bws_beer_page1.json")
    if "liquorland" in u:
        # Product detail pages carry a numeric product-id suffix like
        # `_2605953`; listing pages (e.g. /beer) do not.
        if re.search(r"_\d{5,}", u):
            return load_fixture("liquorland_product_detail.html")
        return load_fixture("liquorland_beer_page1.html")
    return None


class _FakeImpl:
    """Stand-in for a fetcher.FetcherImpl that replays fixtures."""

    def __init__(self, resolver):
        self.resolver = resolver
        self.calls = []

    def fetch_url(self, url):
        self.calls.append(url)
        return self.resolver(url)


@pytest.fixture
def fake_fetchers(monkeypatch):
    """
    Return an installer: `fake_fetchers(resolver=default_resolver)` patches
    every fetch entry point to replay fixtures, and returns a call log.
    """
    def _install(resolver=default_resolver):
        import scraping.fetcher as fetcher_mod
        from scraping.liquorland_processor import LiquorlandProcessor

        impl = _FakeImpl(resolver)
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


# --------------------------------------------------------------------------
# Fake processor for queue/controller-only tests
# --------------------------------------------------------------------------
from scraping.processor import RetailerProcessor  # noqa: E402


class FakeProcessor(RetailerProcessor):
    """
    A canned processor for testing controller/queue orchestration without
    any real parsing. Configure the items it yields, the follow-up page
    metadata, and whether get_items should raise (to exercise retries).
    """

    def __init__(self, items=None, next_metadata=None, detail_tasks=None,
                 raise_on_get_items=False, discover=None, detail_result=None):
        super().__init__()
        self._items = items or []
        self._next_metadata = next_metadata
        self._detail_tasks = detail_tasks or []
        self._raise = raise_on_get_items
        self._discover = discover or []
        self._detail_result = detail_result or {"percent": 5.0, "std_drinks": 1.4}
        self.get_items_calls = 0

    def get_items(self, url, metadata=None):
        self.get_items_calls += 1
        if self._raise:
            raise RuntimeError("boom")
        return list(self._items), self._next_metadata

    def discover_tasks(self, url):
        return list(self._discover)

    def build_detail_tasks(self, items):
        return list(self._detail_tasks)

    def process_drink_detail(self, url, metadata=None):
        return dict(self._detail_result)
