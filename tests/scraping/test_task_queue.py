"""
Offline tests for the scrape task queue in db/databaseHandler.py.

Tier: "Batch / task queue tests (offline, Tier 1 speed)" per
docs/TESTING_DESIGN.md. Exercises the queue functions directly against a
throwaway SQLite DB (the `temp_db` fixture) -- no network, no controller,
no processor involved.
"""
import threading

import db.databaseHandler as dbh


def test_add_then_claim_marks_in_progress(temp_db):
    task_id = dbh.add_scrape_task(
        temp_db, "bws", "http://x/1", {"a": 1}, "run-1", task_type="page"
    )
    task = dbh.get_next_pending_task(temp_db, "bws")
    assert task is not None
    assert task["ID"] == task_id
    assert task["status"] == "in_progress"


def test_second_claim_gets_a_different_task(temp_db):
    id1 = dbh.add_scrape_task(
        temp_db, "bws", "http://x/1", None, "run-1", task_type="page"
    )
    id2 = dbh.add_scrape_task(
        temp_db, "bws", "http://x/2", None, "run-1", task_type="page"
    )

    first = dbh.get_next_pending_task(temp_db, "bws")
    second = dbh.get_next_pending_task(temp_db, "bws")

    assert first["ID"] != second["ID"]
    assert {first["ID"], second["ID"]} == {id1, id2}
    # Queue is now drained -- a third claim returns None.
    assert dbh.get_next_pending_task(temp_db, "bws") is None


def test_full_lifecycle_pending_in_progress_completed(temp_db):
    task_id = dbh.add_scrape_task(
        temp_db, "ll", "http://y/1", None, "run-2", task_type="page"
    )
    assert dbh.get_pending_tasks_count(temp_db, "ll") == 1

    task = dbh.get_next_pending_task(temp_db, "ll")
    assert task["status"] == "in_progress"
    assert dbh.get_pending_tasks_count(temp_db, "ll") == 0

    dbh.update_task_status(temp_db, task_id, "completed")
    assert dbh.get_pending_tasks_count(temp_db, "ll") == 0

    cur = temp_db.cursor()
    cur.execute("SELECT status FROM scrape_tasks WHERE ID = ?", (task_id,))
    assert cur.fetchone()["status"] == "completed"


def test_retry_returns_task_to_pending_and_reclaimable(temp_db):
    task_id = dbh.add_scrape_task(
        temp_db, "bws", "http://x/1", None, "run-3", task_type="page"
    )
    claimed = dbh.get_next_pending_task(temp_db, "bws")
    assert claimed["ID"] == task_id

    dbh.update_task_status(temp_db, task_id, "pending", {"error": "boom"})
    assert dbh.get_pending_tasks_count(temp_db, "bws") == 1

    reclaimed = dbh.get_next_pending_task(temp_db, "bws")
    assert reclaimed["ID"] == task_id
    assert reclaimed["status"] == "in_progress"


def test_reset_in_progress_tasks_recovers_stuck_rows(temp_db):
    dbh.add_scrape_task(
        temp_db, "bws", "http://x/1", None, "run-4", task_type="page"
    )
    dbh.add_scrape_task(
        temp_db, "bws", "http://x/2", None, "run-4", task_type="page"
    )
    dbh.get_next_pending_task(temp_db, "bws")  # claims task 1 -> in_progress
    dbh.get_next_pending_task(temp_db, "bws")  # claims task 2 -> in_progress
    assert dbh.get_pending_tasks_count(temp_db, "bws") == 0

    recovered = dbh.reset_in_progress_tasks(temp_db, retailer="bws")
    assert recovered == 2
    assert dbh.get_pending_tasks_count(temp_db, "bws") == 2


def test_pending_counts_mixed_retailers_and_runs(temp_db):
    dbh.add_scrape_task(
        temp_db, "bws", "http://x/1", None, "run-a", task_type="page"
    )
    dbh.add_scrape_task(
        temp_db, "bws", "http://x/2", None, "run-a", task_type="page"
    )
    dbh.add_scrape_task(
        temp_db, "ll", "http://y/1", None, "run-a", task_type="page"
    )
    dbh.add_scrape_task(
        temp_db, "ll", "http://y/2", None, "run-b", task_type="page"
    )

    assert dbh.get_pending_tasks_count(temp_db) == 4
    assert dbh.get_pending_tasks_count(temp_db, "bws") == 2
    assert dbh.get_pending_tasks_count(temp_db, "ll") == 2

    assert dbh.get_pending_tasks_count_by_run(temp_db, "run-a") == 3
    assert dbh.get_pending_tasks_count_by_run(temp_db, "run-a", "bws") == 2
    assert dbh.get_pending_tasks_count_by_run(temp_db, "run-a", "ll") == 1
    assert dbh.get_pending_tasks_count_by_run(temp_db, "run-b", "ll") == 1
    assert dbh.get_pending_tasks_count_by_run(temp_db, "run-b", "bws") == 0


def test_drink_detail_tasks_are_prioritised_over_page_tasks(temp_db):
    # get_next_pending_task orders by
    # `CASE task_type WHEN 'drink_detail' THEN 0 ELSE 1 END` first, so a
    # pending drink_detail task jumps ahead of an older pending page task.
    page_id = dbh.add_scrape_task(
        temp_db, "ll", "http://y/page", None, "run-5", task_type="page"
    )
    detail_id = dbh.add_scrape_task(
        temp_db,
        "ll",
        "http://y/detail",
        None,
        "run-5",
        task_type="drink_detail",
    )

    first = dbh.get_next_pending_task(temp_db, "ll")
    assert first["ID"] == detail_id
    assert first["task_type"] == "drink_detail"

    second = dbh.get_next_pending_task(temp_db, "ll")
    assert second["ID"] == page_id


def test_get_next_pending_task_by_run_also_prioritises_drink_detail(
    temp_db,
):
    dbh.create_run(temp_db, "run-6", retailer="ll", category=None)
    dbh.add_scrape_task(
        temp_db, "ll", "http://y/page", None, "run-6", task_type="page"
    )
    detail_id = dbh.add_scrape_task(
        temp_db,
        "ll",
        "http://y/detail",
        None,
        "run-6",
        task_type="drink_detail",
    )

    first = dbh.get_next_pending_task_by_run(temp_db, "run-6", "ll")
    assert first["ID"] == detail_id

    # Scoped to a different run, nothing should be claimable.
    assert dbh.get_next_pending_task_by_run(temp_db, "run-other", "ll") is None


def test_concurrent_claim_race_no_double_claims(temp_db):
    """
    Seeds a pool of pending tasks and races several threads -- each on its
    own connection -- to drain the queue via get_next_pending_task. Every
    task id must be claimed exactly once: no double-claims, none lost.
    This is the regression net for the BEGIN IMMEDIATE atomic claim in
    db.databaseHandler._claim_next_pending_task.
    """
    num_tasks = 20
    num_threads = 4

    seeded_ids = {
        dbh.add_scrape_task(
            temp_db,
            "bws",
            f"http://race/{i}",
            None,
            "run-race",
            task_type="page",
        )
        for i in range(num_tasks)
    }

    claimed_ids = []
    lock = threading.Lock()
    errors = []

    def worker():
        conn = dbh.create_connection()
        try:
            while True:
                task = dbh.get_next_pending_task(conn, "bws")
                if task is None:
                    break
                with lock:
                    claimed_ids.append(task["ID"])
        except Exception as e:  # pragma: no cover - surfaced via `errors`
            errors.append(e)
        finally:
            conn.close()

    threads = [threading.Thread(target=worker) for _ in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert sorted(claimed_ids) == sorted(seeded_ids)
    assert len(claimed_ids) == len(set(claimed_ids))
