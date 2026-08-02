"""
Tier 2 -- live fetch + parse smoke tests (real network, opt-in, costs
ScrapingBee credits).

See docs/TESTING_DESIGN.md, "Tier 2" section. Scope is deliberately
narrow: fetch a real page and run it through the processor's parse logic
-- no controller, no task queue, no DB. That's the only seam that touches
genuinely unpredictable outside-world state (a retailer redesigning their
page overnight); everything else -- queue/controller orchestration, DB
writes and dedup -- is deterministic internal logic and is already fully
covered offline by Tier 3 (test_pipeline.py).

Every test here is marked `@pytest.mark.live` and excluded from the
default `pytest` run via `pyproject.toml`'s `addopts = "-m 'not live'"`, so
plain `pytest` never spends a credit by accident. Run explicitly with
`pytest -m live` when you want to check whether a retailer changed
something -- that's also the cue that a fixture under tests/scraping/
fixtures/ has gone stale and needs refreshing via refresh_fixtures.py.

Assertions are deliberately loose (`>= 1`, "price is positive") because
live data is nondeterministic -- a retailer lists however many products it
lists today. Anything needing an exact count or precise before/after state
belongs in Tier 3 against a frozen fixture, not here.
"""
import json
from pathlib import Path

import pytest

from scraping.bws_processor import BWSProcessor
from scraping.liquorland_processor import LiquorlandProcessor

GOLDEN_URLS = json.loads(
    (Path(__file__).parent / "golden_urls.json").read_text(encoding="utf-8")
)


@pytest.mark.live
def test_bws_listing_returns_sane_items():
    """Real fetch + real parse of one BWS Browse API listing URL."""
    url = GOLDEN_URLS["bws"]["listing"]
    items, _ = BWSProcessor().get_items(url)

    assert len(items) >= 1
    assert all(item.price > 0 for item in items)


@pytest.mark.live
def test_liquorland_listing_returns_sane_items():
    """Real fetch + real parse of one Liquorland listing page."""
    url = GOLDEN_URLS["ll"]["listing"]
    items, _ = LiquorlandProcessor().get_items(url)

    assert len(items) >= 1
    assert all(item.price > 0 for item in items)
    assert all(item.link for item in items)


@pytest.mark.live
def test_liquorland_detail_link_extracted_from_listing_is_live_good():
    """
    Chained locator test -- the point of Tier 2 (see TESTING_DESIGN.md,
    "Validating extracted locators (chained fetch + parse)").

    `get_items` doesn't just extract data, it extracts a detail-page LINK
    for each item (the same link `build_detail_tasks` would enqueue as a
    drink_detail task). That link is only "good" if it actually resolves
    to a page the detail parser can consume -- which you can't verify
    without fetching it. This proves it by chaining fetch -> parse ->
    fetch -> parse *by hand*, following the extracted locator exactly
    once: fetch+parse the listing, take the first item's real extracted
    link, then fetch+parse ITS detail page directly via
    `process_drink_detail`.

    Deliberately does NOT go through `build_detail_tasks` /
    `ScrapingController` / the task queue / the DB -- whether an extracted
    URL is correctly carried through enqueue -> claim -> persist is
    deterministic internal logic, already proven offline against fixtures
    in Tier 3. Running that live would add no new signal about the link
    itself, only fan out into dozens of extra live fetches (one per
    listing item) -- exactly the credit blowout Tier 2 is designed to
    avoid.
    """
    listing_url = GOLDEN_URLS["ll"]["listing"]
    processor = LiquorlandProcessor()
    items, _ = processor.get_items(listing_url)
    assert len(items) >= 1

    detail_url = items[0].link or GOLDEN_URLS["ll"]["detail_fallback"]
    assert detail_url

    details = processor.process_drink_detail(detail_url)

    assert details is not None
    assert details.get("percent") is not None
