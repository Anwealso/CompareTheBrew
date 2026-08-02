"""
Tier 1 parsing tests for BWSProcessor. `fake_fetchers()` replays the
checked-in real BWS Browse API fixture (tests/scraping/fixtures/
bws_beer_page1.json) offline, so `get_items` / `discover_tasks` exercise
the real parsing logic against real captured data with zero network.
"""
from scraping.bws_processor import BWSProcessor

LISTING_URL = "https://api.bws.com.au/apis/ui/Browse?department=beer"


def test_get_items_parses_all_products_from_fixture(fake_fetchers):
    fake_fetchers()
    items, next_metadata = BWSProcessor().get_items(LISTING_URL)

    assert len(items) > 0
    assert next_metadata is None


def test_get_items_every_item_is_bws_store(fake_fetchers):
    fake_fetchers()
    items, _ = BWSProcessor().get_items(LISTING_URL)

    assert all(i.store == "bws" for i in items)


def test_get_items_every_item_has_positive_price(fake_fetchers):
    fake_fetchers()
    items, _ = BWSProcessor().get_items(LISTING_URL)

    assert all(i.price > 0 for i in items)


def test_get_items_links_point_at_bws_product_pages(fake_fetchers):
    fake_fetchers()
    items, _ = BWSProcessor().get_items(LISTING_URL)

    assert all(
        i.link.startswith("https://bws.com.au/product/") for i in items
    )


def test_get_items_alcoholic_items_have_percent_and_std_drinks(
    fake_fetchers,
):
    fake_fetchers()
    items, _ = BWSProcessor().get_items(LISTING_URL)

    # BWS carries ABV/std drinks directly in the listing JSON (one-phase
    # scrape), so every non-zero-alc item should already be populated.
    alcoholic_items = [i for i in items if not i.zero_alc]
    assert len(alcoholic_items) > 0
    assert all(i.percent > 0 for i in alcoholic_items)
    assert all(i.stdDrinks > 0 for i in alcoholic_items)


def test_get_items_pack_qty_is_a_positive_int(fake_fetchers):
    fake_fetchers()
    items, _ = BWSProcessor().get_items(LISTING_URL)

    assert all(isinstance(i.pack_qty, int) for i in items)
    assert all(i.pack_qty > 0 for i in items)


def test_discover_tasks_returns_at_least_one_page_task(fake_fetchers):
    fake_fetchers()
    tasks = BWSProcessor().discover_tasks(
        "https://api.bws.com.au/apis/ui/Browse?department=beer"
        "&pageNumber=1&pageSize=1000"
    )

    assert len(tasks) >= 1
    assert "url" in tasks[0]
    assert "metadata" in tasks[0]


def test_discover_tasks_total_product_count_key_is_absent_from_real_api(
    fake_fetchers,
):
    """
    Documents a latent bug: discover_tasks reads data["TotalProductCount"],
    but the real BWS Browse API response (see the fixture) only carries
    "TotalRecordCount". So `total_count` always reads as 0 against real
    data and discover_tasks silently falls back to a single page rather
    than paginating off the true product count. This test pins the
    *current* (buggy) behaviour rather than the intended one; the fixture
    happens to have TotalRecordCount=564 which is still < 1000, so the
    fallback-to-one-page result is coincidentally the same as the correct
    answer here, but the computation is wrong for any total >= 1000.
    """
    from tests.scraping._support import load_fixture
    import json

    fixture = json.loads(load_fixture("bws_beer_page1.json"))
    assert "TotalProductCount" not in fixture
    assert "TotalRecordCount" in fixture and fixture["TotalRecordCount"] > 0

    tasks = BWSProcessor().discover_tasks(
        "https://api.bws.com.au/apis/ui/Browse?department=beer"
        "&pageNumber=1&pageSize=1000"
    )
    # Falls back to exactly one page because TotalProductCount is missing
    # (reads as 0), not because the real product count actually fits on
    # one page.
    assert len(tasks) == 1
    assert tasks[0]["metadata"]["page"] == 1
