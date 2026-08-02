"""
Importable helpers shared across the scraping test suite.

Import from tests like:
    from tests.scraping._support import FakeProcessor, load_fixture, default_resolver

Fixtures (temp_db, fake_fetchers) live in conftest.py; these are plain
callables/classes so they can be imported directly.
"""
import re
from pathlib import Path

from scraping.processor import RetailerProcessor

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


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


class FakeImpl:
    """Stand-in for a fetcher.FetcherImpl that replays fixtures."""

    def __init__(self, resolver):
        self.resolver = resolver
        self.calls = []

    def fetch_url(self, url):
        self.calls.append(url)
        return self.resolver(url)


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
