"""
Tier 3 -- fixture-driven pipeline / end-to-end tests (offline, deterministic).

See docs/TESTING_DESIGN.md, "Tier 3" section: this is the home for exact
task-count assertions and the interim state of the `drinks` table between
the Liquorland listing pass and its detail-page backfill. It runs the REAL
ScrapingController against the REAL bws/ll processors and a REAL SQLite DB
(via the `temp_db` fixture) -- only the fetch step is faked
(`fake_fetchers`), so the whole queue -> processor -> DB pipeline is
exercised end to end with zero network and zero credits.
"""
from db.databaseHandler import add_scrape_task, create_connection
from scraping.controller import ScrapingController
from scraping.liquorland_processor import LiquorlandProcessor

LL_LISTING_URL = "https://www.liquorland.com.au/beer"
BWS_LISTING_URL = (
    "https://api.bws.com.au/apis/ui/Browse?Location=%2Fbeer%2Fall-beer"
    "&banner=true&department=beer&pageNumber=1&pageSize=12"
    "&sortType=Browse_Relevance_LocalSales&subDepartment=all+beer"
)


def _fresh_conn():
    """
    Open a brand-new connection to the same throwaway SQLite file that
    `temp_db` pointed SQLITE_DB_PATH at. The controller opens, commits, and
    closes its own connections per `run_next()` call, so reads for
    assertions go through a fresh connection rather than risking a stale
    read on the long-lived `temp_db` connection.
    """
    return create_connection()


def test_liquorland_two_phase_pipeline(temp_db, fake_fetchers):
    """
    Drives the REAL controller + LiquorlandProcessor + SQLite DB through
    both phases of a Liquorland scrape.

    Phase 1 (listing pass): seed one 'page' task, run it once, and assert
    the INTERIM state -- one placeholder drinks row per product tile
    (percent/stdDrinks/score all "not yet scraped", zero_alc NOT
    mislabeled), and exactly one pending drink_detail task per tile (none
    cached in a fresh DB), zero follow-up page tasks (the fixture's
    get_items returns no next_metadata).

    Phase 2 (detail drain): process every drink_detail task and assert the
    FINAL state -- rows are fully populated, row COUNT IS UNCHANGED
    (detail backfill UPDATEs the existing (store, link, pack_qty) row
    rather than inserting a new one), and every task is 'completed'.
    """
    fake_fetchers()

    # N = number of product tiles in the fixture, established via the real
    # processor rather than hand-counted, so it can't drift from the
    # fixture.
    items, _ = LiquorlandProcessor().get_items(LL_LISTING_URL)
    n = len(items)
    assert n > 0

    controller = ScrapingController()
    add_scrape_task(temp_db, "ll", LL_LISTING_URL, {"page": 1}, None, "page")
    temp_db.commit()

    # --- Phase 1: process the single 'page' task ---
    result = controller.run_next("ll")
    assert result is not None
    assert result["task_type"] == "page"
    assert result["success"] is True

    conn = _fresh_conn()
    try:
        drinks = conn.execute("SELECT * FROM drinks").fetchall()
        assert len(drinks) == n

        for row in drinks:
            assert row["percent"] == 0.0
            assert row["stdDrinks"] == 0.0
            assert row["score"] is None
            assert row["zero_alc"] == 0

        pending_detail = conn.execute(
            "SELECT COUNT(*) FROM scrape_tasks "
            "WHERE task_type = 'drink_detail' AND status = 'pending'"
        ).fetchone()[0]
        assert pending_detail == n

        pending_page = conn.execute(
            "SELECT COUNT(*) FROM scrape_tasks "
            "WHERE task_type = 'page' AND status = 'pending'"
        ).fetchone()[0]
        assert pending_page == 0
    finally:
        conn.close()

    # --- Phase 2: drain every drink_detail task ---
    guard = 0
    while True:
        guard += 1
        assert guard <= n + 5, "drain loop did not terminate"
        if controller.run_next("ll") is None:
            break

    conn = _fresh_conn()
    try:
        drinks = conn.execute("SELECT * FROM drinks").fetchall()
        # Row count is unchanged: the (store, link, pack_qty) dedup key
        # means the detail backfill UPDATEs the existing row rather than
        # inserting a second one.
        assert len(drinks) == n

        for row in drinks:
            assert row["percent"] > 0
            assert row["stdDrinks"] > 0
            assert row["score"] is not None
            # The fixture's detail page carries percent=3.5, which is not
            # zero-alc.
            assert row["zero_alc"] == 0

        pending = conn.execute(
            "SELECT COUNT(*) FROM scrape_tasks WHERE status = 'pending'"
        ).fetchone()[0]
        assert pending == 0

        not_completed = conn.execute(
            "SELECT COUNT(*) FROM scrape_tasks WHERE status != 'completed'"
        ).fetchone()[0]
        assert not_completed == 0
    finally:
        conn.close()


def test_bws_one_phase_pipeline(temp_db, fake_fetchers):
    """
    Pins the opposite branch from the Liquorland test above: BWS is
    one-phase. Processing a single 'page' task must fully populate
    percent/stdDrinks/score immediately (BWS carries ABV/std drinks in the
    listing JSON itself) and must create ZERO drink_detail follow-up
    tasks -- the controller's retailer-specific branch
    (`if retailer_name == "ll": ...`) only builds detail tasks for
    Liquorland.
    """
    fake_fetchers()
    add_scrape_task(
        temp_db, "bws", BWS_LISTING_URL, {"page": 1}, None, "page"
    )
    temp_db.commit()

    controller = ScrapingController()
    result = controller.run_next("bws")
    assert result is not None
    assert result["task_type"] == "page"
    assert result["success"] is True

    conn = _fresh_conn()
    try:
        drinks = conn.execute("SELECT * FROM drinks").fetchall()
        assert len(drinks) > 0

        alcoholic = [d for d in drinks if not d["zero_alc"]]
        assert len(alcoholic) > 0
        for row in alcoholic:
            assert row["percent"] > 0
            assert row["stdDrinks"] > 0
            assert row["score"] is not None

        detail_tasks = conn.execute(
            "SELECT COUNT(*) FROM scrape_tasks "
            "WHERE task_type = 'drink_detail'"
        ).fetchone()[0]
        assert detail_tasks == 0
    finally:
        conn.close()


def test_liquorland_parallel_workers_match_single_worker_run(
    temp_db, fake_fetchers
):
    """
    Concurrency regression check with real work (complementing the pure
    claim-race unit test in the offline queue tests): seed the same LL
    'page' task and drain it with 4 worker threads via
    `controller.run_parallel`, and assert the end state matches the
    single-worker drain in `test_liquorland_two_phase_pipeline` above --
    same row count N, every task 'completed', and no duplicate rows or
    tasks from a double-claim. Ran repeatedly (5x) during development
    without flaking; SQLite's default connection `timeout` (5s busy-wait)
    is enough headroom for this task volume.
    """
    fake_fetchers()
    items, _ = LiquorlandProcessor().get_items(LL_LISTING_URL)
    n = len(items)
    assert n > 0

    controller = ScrapingController()
    add_scrape_task(temp_db, "ll", LL_LISTING_URL, {"page": 1}, None, "page")
    temp_db.commit()

    result = controller.run_parallel(
        num_workers=4, retailer="ll", run_id=None, limit=None
    )
    assert result["pending"] == 0

    conn = _fresh_conn()
    try:
        drinks = conn.execute("SELECT * FROM drinks").fetchall()
        assert len(drinks) == n  # no duplicate rows from a double-claim

        for row in drinks:
            assert row["percent"] > 0
            assert row["stdDrinks"] > 0
            assert row["score"] is not None

        not_completed = conn.execute(
            "SELECT COUNT(*) FROM scrape_tasks WHERE status != 'completed'"
        ).fetchone()[0]
        assert not_completed == 0

        total_tasks = conn.execute(
            "SELECT COUNT(*) FROM scrape_tasks"
        ).fetchone()[0]
        # 1 page task + n drink_detail tasks, no duplicates enqueued.
        assert total_tasks == n + 1
    finally:
        conn.close()
