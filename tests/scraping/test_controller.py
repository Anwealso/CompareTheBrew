"""
Offline tests for ScrapingController orchestration (discover / run_next)
against a hand-written FakeProcessor -- no network, no real retailer
parsing. Tier: "Batch / task queue tests (offline, Tier 1 speed)" per
docs/TESTING_DESIGN.md.

NOTE: run_next() previously mis-indexed the claimed task tuple (it read
metadata from the task_type column), which crashed every task with a
JSONDecodeError. That bug is now fixed in scraping/controller.py -- the
indices match scrape_tasks schema order:
    ID(0) retailer(1) url(2) status(3) task_type(4) metadata(5) run_id(6)
    attempts(7) created_at(8) updated_at(9)
The run_next() tests below assert the real, intended contract.
"""
import db.databaseHandler as dbh
from entities.drink_item import DrinkItem
from scraping.controller import ScrapingController
from tests.scraping._support import FakeProcessor


def make_drink(link="http://x/1", store="bws", pack_qty=1, price=10.0):
    return DrinkItem(
        store=store,
        brand="BrandX",
        name="NameX",
        type="beer",
        price=price,
        link=link,
        ml=375,
        percent=4.5,
        std_drinks=1.4,
        pack_qty=pack_qty,
        score=None,
        image="http://img",
        promotion=None,
        old_price=None,
    )


def new_controller(**kwargs):
    # Point at a sitemaps file that doesn't exist so ScrapingController
    # falls back to its empty default {"bws": [], "ll": [], "fc": []}.
    # Tests then set controller.sitemaps[...] explicitly, giving full
    # control over seed URLs independent of the real
    # scraping/sitemaps.json contents.
    kwargs.setdefault("sitemaps_file", "does-not-exist.json")
    return ScrapingController(**kwargs)


def _seed_page_task(
    retailer, url="http://x/page1", metadata=None, run_id="run-x"
):
    conn = dbh.create_connection()
    dbh.create_run(conn, run_id, retailer=retailer, category=None)
    task_id = dbh.add_scrape_task(
        conn, retailer, url, metadata, run_id, task_type="page"
    )
    conn.close()
    return task_id, run_id


# ---------------------------------------------------------------------------
# discover() -- does not touch run_next(), unaffected by the bug above.
# ---------------------------------------------------------------------------


def test_discover_seeds_exactly_the_tasks_the_processor_returns(temp_db):
    controller = new_controller()
    controller.sitemaps["bws"] = ["http://seed/listing"]
    controller.processors["bws"] = FakeProcessor(
        discover=[
            {"url": "http://x/page1", "metadata": {"p": 1}},
            {"url": "http://x/page2", "metadata": {"p": 2}},
            {"url": "http://x/page3", "metadata": None},
        ]
    )

    run_id = controller.discover("bws")

    assert run_id is not None
    assert dbh.get_pending_tasks_count_by_run(temp_db, run_id, "bws") == 3

    cur = temp_db.cursor()
    cur.execute(
        "SELECT task_type, url FROM scrape_tasks WHERE run_id = ? "
        "ORDER BY ID",
        (run_id,),
    )
    rows = cur.fetchall()
    assert [r["task_type"] for r in rows] == ["page", "page", "page"]
    assert [r["url"] for r in rows] == [
        "http://x/page1",
        "http://x/page2",
        "http://x/page3",
    ]


def test_discover_fans_out_across_multiple_seed_urls(temp_db):
    # discover() calls discover_tasks() once per seed URL configured in
    # self.sitemaps[retailer]; the FakeProcessor returns the same canned
    # pair each time, so two seed URLs should yield 2 * 2 = 4 tasks.
    controller = new_controller()
    controller.sitemaps["ll"] = ["http://seed/1", "http://seed/2"]
    controller.processors["ll"] = FakeProcessor(
        discover=[
            {"url": "http://y/a", "metadata": None},
            {"url": "http://y/b", "metadata": None},
        ]
    )

    run_id = controller.discover("ll")

    assert dbh.get_pending_tasks_count_by_run(temp_db, run_id, "ll") == 4


def test_discover_unknown_retailer_seeds_nothing(temp_db):
    controller = new_controller()
    result = controller.discover("nonexistent-retailer")
    assert result is None
    assert dbh.get_pending_tasks_count(temp_db) == 0


# ---------------------------------------------------------------------------
# run_next() -- exercises the fixed controller against a FakeProcessor.
# ---------------------------------------------------------------------------


def test_run_next_bws_page_task_inserts_drinks_and_completes(temp_db):
    """BWS is one-phase: a page task inserts drinks directly and creates
    zero drink_detail follow-up tasks."""
    controller = new_controller()
    items = [make_drink(link="http://x/1"), make_drink(link="http://x/2")]
    controller.processors["bws"] = FakeProcessor(items=items)
    task_id, run_id = _seed_page_task("bws")

    result = controller.run_next("bws", run_id)

    assert result["success"] is True
    assert result["task_id"] == task_id

    cur = temp_db.cursor()
    cur.execute("SELECT status FROM scrape_tasks WHERE ID = ?", (task_id,))
    assert cur.fetchone()["status"] == "completed"

    cur.execute("SELECT COUNT(*) AS c FROM drinks")
    assert cur.fetchone()["c"] == len(items)

    cur.execute(
        "SELECT COUNT(*) AS c FROM scrape_tasks "
        "WHERE task_type = 'drink_detail'"
    )
    assert cur.fetchone()["c"] == 0


def test_run_next_ll_page_task_enqueues_drink_detail_tasks(temp_db):
    """Liquorland is two-phase: a page task enqueues one drink_detail
    follow-up task per entry returned by build_detail_tasks()."""
    controller = new_controller()
    items = [make_drink(store="ll", link="http://ll/1")]
    detail_tasks = [
        {"url": "http://ll/1/detail", "metadata": {"link": "http://ll/1"}},
        {"url": "http://ll/2/detail", "metadata": {"link": "http://ll/2"}},
    ]
    controller.processors["ll"] = FakeProcessor(
        items=items, detail_tasks=detail_tasks
    )
    _task_id, run_id = _seed_page_task("ll")

    controller.run_next("ll", run_id)

    cur = temp_db.cursor()
    cur.execute(
        "SELECT COUNT(*) AS c FROM scrape_tasks "
        "WHERE task_type = 'drink_detail'"
    )
    assert cur.fetchone()["c"] == len(detail_tasks)


def test_run_next_next_metadata_enqueues_followup_page_task(temp_db):
    controller = new_controller()
    controller.processors["bws"] = FakeProcessor(
        items=[make_drink()],
        next_metadata={"next_url": "http://x/page2"},
    )
    _task_id, run_id = _seed_page_task("bws")

    controller.run_next("bws", run_id)

    cur = temp_db.cursor()
    cur.execute(
        "SELECT COUNT(*) AS c FROM scrape_tasks "
        "WHERE task_type = 'page' AND url = 'http://x/page2'"
    )
    assert cur.fetchone()["c"] == 1


def test_run_next_retry_then_fail_on_exhausted_attempts(temp_db):
    controller = new_controller(max_retries=2)
    controller.processors["bws"] = FakeProcessor(raise_on_get_items=True)
    task_id, run_id = _seed_page_task("bws")

    # Attempt 1/2: still under max_retries -> back to pending.
    controller.run_next("bws", run_id)
    cur = temp_db.cursor()
    cur.execute(
        "SELECT status, attempts FROM scrape_tasks WHERE ID = ?",
        (task_id,),
    )
    row = cur.fetchone()
    assert row["status"] == "pending"
    assert row["attempts"] == 1

    # Attempt 2/2: attempts hits max_retries -> failed.
    controller.run_next("bws", run_id)
    cur.execute(
        "SELECT status, attempts FROM scrape_tasks WHERE ID = ?",
        (task_id,),
    )
    row = cur.fetchone()
    assert row["status"] == "failed"
    assert row["attempts"] == 2
