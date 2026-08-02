"""
Tier 1 parsing tests for LiquorlandProcessor. `fake_fetchers()` replays
the checked-in real Liquorland listing/detail fixtures offline. Tests
that go through `get_details_from_item_page` / `build_detail_tasks` also
need `temp_db`, since those methods call `_get_cached_details`, which
opens a real DB connection (`db.databaseHandler.create_connection`) to
check for a cached row. `temp_db` isolates that behind a throwaway
SQLite file so nothing touches the real dev DB, and the cache starts
empty so the fixture-backed fetch path always runs.
"""
from scraping.liquorland_processor import LiquorlandProcessor

LISTING_URL = "https://www.liquorland.com.au/beer"
DETAIL_URL = (
    "https://www.liquorland.com.au/beer-and-cider/"
    "great-northern-super-crisp-lager-block-can-375ml_2605953"
)


def test_get_items_parses_all_tiles_from_fixture(fake_fetchers):
    fake_fetchers()
    items, next_metadata = LiquorlandProcessor().get_items(LISTING_URL)

    assert len(items) > 0
    assert next_metadata is None


def test_get_items_every_item_is_liquorland_store(fake_fetchers):
    fake_fetchers()
    items, _ = LiquorlandProcessor().get_items(LISTING_URL)

    assert all(i.store == "liquorland" for i in items)


def test_get_items_every_item_has_positive_price(fake_fetchers):
    fake_fetchers()
    items, _ = LiquorlandProcessor().get_items(LISTING_URL)

    assert all(i.price > 0 for i in items)


def test_get_items_every_item_has_a_valid_link(fake_fetchers):
    fake_fetchers()
    items, _ = LiquorlandProcessor().get_items(LISTING_URL)

    assert all(i.link for i in items)
    assert all(i.link.startswith("http") for i in items)


def test_get_items_listing_pass_leaves_alcohol_fields_in_interim_state(
    fake_fetchers,
):
    """
    Percent/std drinks aren't on the listing tile; only the later
    drink_detail pass fills them in. Crucially, the processor does NOT
    run is_zero_alc() on this placeholder 0.0 (that would return True
    and mislabel every Liquorland product as zero-alcohol until its
    detail task lands) so zero_alc must stay False here too.
    """
    fake_fetchers()
    items, _ = LiquorlandProcessor().get_items(LISTING_URL)

    assert all(i.percent == 0.0 for i in items)
    assert all(i.stdDrinks == 0.0 for i in items)
    assert all(i.zero_alc is False for i in items)


def test_get_details_from_item_page_parses_real_fixture(
    fake_fetchers, temp_db
):
    fake_fetchers()
    details = LiquorlandProcessor().get_details_from_item_page(DETAIL_URL)

    assert details is not None
    assert details["percent"] is not None
    assert details["percent"] > 0
    assert details["std_drinks"] >= 0


def test_build_detail_tasks_returns_one_task_per_item(
    fake_fetchers, temp_db
):
    fake_fetchers()
    proc = LiquorlandProcessor()
    items, _ = proc.get_items(LISTING_URL)

    tasks = proc.build_detail_tasks(items)

    assert len(tasks) > 0
    for task in tasks:
        assert "url" in task
        assert "metadata" in task
        assert "store" in task["metadata"]
        assert "link" in task["metadata"]
        assert "pack_qty" in task["metadata"]
