import json
import os
import uuid
import threading
import queue
import time
from datetime import datetime
from typing import Dict, List
from scraping.processor import RetailerProcessor
from scraping.bws_processor import BWSProcessor
from scraping.liquorland_processor import LiquorlandProcessor
from db.databaseHandler import (
    create_connection, upsert_source, dbhandler, 
    add_scrape_task, get_next_pending_task, get_next_pending_task_by_run,
    update_task_status, get_pending_tasks_count, get_pending_tasks_count_by_run,
    increment_task_attempts, create_run, update_drink_details,
    reset_in_progress_tasks
)

NUM_WORKERS = 4


class ScrapingController:
    """
    Central controller for coordinating the scraping pipeline across different retailers.
    Uses a Task Queue system with a progressive iterator approach.
    """
    def __init__(self, sitemaps_file: str = "scraping/sitemaps.json", max_retries: int = 3):
        """
        Initializes the controller with a sitemaps file and retry limit.

        Args:
            sitemaps_file (str): Path to the JSON file containing retailer URLs.
            max_retries (int): Maximum number of times a task can be attempted before failing.
        """
        print(f"[temp_scraper_debug] enter ScrapingController.__init__(sitemaps_file={sitemaps_file}, max_retries={max_retries})")  # TODO: Remove this temp_scraper_debug print info.
        self.sitemaps_file = sitemaps_file
        self.max_retries = max_retries
        self.progress_callback = None
        self.page_items_callback = None
        self.drink_callback = None
        self.current_url = ""
        self._drink_counter = 0
        self.last_task_type = None
        self.processors: Dict[str, RetailerProcessor] = {
            "bws": BWSProcessor(),
            "ll": LiquorlandProcessor(),
        }
        self.sitemaps = self._load_sitemaps()
        self._stop_workers = False
        self._event_queue: queue.Queue | None = None
        self._run_thread: threading.Thread | None = None
        self._page_tasks_completed = 0
        self._detail_tasks_completed = 0
        self._drinks_processed = 0
        self._limit = None
        self._current_run_id = None
        self._active_retailer = None
        self._stats_lock = threading.Lock()

    def _load_sitemaps(self) -> Dict[str, List[str]]:
        """
        Loads the sitemaps configuration from disk.
        """
        if not os.path.exists(self.sitemaps_file):
            print(f"Sitemaps file {self.sitemaps_file} not found. Using default structure.")
            return {"bws": [], "ll": [], "fc": []}
        
        with open(self.sitemaps_file, 'r') as f:
            return json.load(f)

    def _emit_event(self, event: dict):
        """Emit an event into the controller's event queue."""
        if self._event_queue is not None:
            self._event_queue.put(event)


    def discover(self, retailer_name: str = None, category: str = None, run_id: str = None):
        """
        Discovery Phase: Hits the retailer's seed URLs once to seed the task queue.
        For pagination retailers (like BWS), it adds all pages at once.
        For others, it adds the first page and depends on the processor to find 'next'.
        
        Args:
            retailer_name: The retailer to discover tasks for (e.g., 'bws', 'll', 'fc')
            category: Optional category to limit discovery to (e.g., 'beer', 'wine', 'spirits', 'premix')
            run_id: Optional UUID for this discovery run. If not provided, a new one is generated.

        TODO: Refactor to allow bulk discovery for all retailers at once and make retailer_name optional.
        """
        print(f"[temp_scraper_debug] enter ScrapingController.discover(retailer={retailer_name}, category={category}, run_id={run_id})")  # TODO: Remove this temp_scraper_debug print info.
        retailer_name = retailer_name.lower()
        if retailer_name not in self.processors:
            print(f"No processor found for retailer: {retailer_name}")
            return

        if run_id is None:
            run_id = str(uuid.uuid4())
            print(f"Generated new run_id: {run_id}")
        
        processor = self.processors[retailer_name]
        seed_urls = self.sitemaps.get(retailer_name, [])
                
        if category:
            seed_urls = [url for url in seed_urls if category in url.lower()]
        
        conn = create_connection()
        if not conn:
            return

        create_run(conn, run_id, retailer=retailer_name, category=category)
        
        print(f"Starting discovery for {retailer_name}" + (f" ({category})" if category else "") + f" (run_id: {run_id})...")
        for url in seed_urls:
            print(f"Discovering from: {url}")
            tasks = processor.discover_tasks(url)
            print(f"Found {len(tasks)} tasks.")
            for t in tasks:
                add_scrape_task(conn, retailer_name, t["url"], t["metadata"], run_id, task_type='page')
                print(f"  - Queued task: {t['url']}")
        
        conn.close()
        return run_id

    def run_next(self, retailer_name: str, run_id: str = None):
        """
        Progressive execution: Pulls ONE task from the queue, processes it, and returns task metadata
        for downstream coordination.
        """
        print(f"[temp_scraper_debug] enter ScrapingController.run_next(retailer={retailer_name}, run_id={run_id})")  # TODO: Remove this temp_scraper_debug print info.
        retailer_name = retailer_name.lower()
        conn = create_connection()
        if not conn:
            return None

        if run_id:
            task = get_next_pending_task_by_run(conn, run_id, retailer_name)
        else:
            task = get_next_pending_task(conn, retailer_name)
        
        if not task:
            self.last_task_type = None
            conn.close()
            return None

        task_id = task[0]
        url = task[2]
        metadata_str = task[4]
        run_id = task[5] if len(task) > 5 else None
        current_attempts = task[6] if len(task) > 6 else 0
        task_type = task[9] if len(task) > 9 else 'page'
        self.last_task_type = task_type
        
        metadata = json.loads(metadata_str) if metadata_str else {}
        processor = self.processors[retailer_name]
        processor.progress_callback = self.progress_callback

        self._emit_event({
            "type": "task_started",
            "task_id": task_id,
            "url": url,
            "task_type": task_type,
            "run_id": run_id,
            "attempt": current_attempts + 1
        })

        print(f"Processing Task {task_id} (Type: {task_type}) (Attempt {current_attempts + 1}/{self.max_retries}): {url}")
        self.current_url = url
        
        increment_task_attempts(conn, task_id)
        
        now = datetime.now().isoformat()
        upsert_source(conn, url, retailer_name, now)
        
        success = False
        try:
            if task_type == 'drink_detail':
                print(f"[temp_scraper_debug] processing drink_detail task for url={url}")  # TODO: Remove this temp_scraper_debug print info.
                details = processor.process_drink_detail(url, metadata)
                print(f"Got details: percent={details.get('percent')}, std_drinks={details.get('std_drinks')}")
                if metadata:
                    store = metadata.get("store", retailer_name)
                    link = metadata.get("link")
                    pack_qty_raw = metadata.get("pack_qty", 1)
                    try:
                        pack_qty = int(float(pack_qty_raw)) if pack_qty_raw is not None else 1
                    except (TypeError, ValueError):
                        pack_qty = 1
                    percent = details.get("percent", 0.0)
                    std_drinks = details.get("std_drinks", 0.0)
                    if link:
                        update_drink_details(conn, store, link, percent, std_drinks, pack_qty)
                update_task_status(conn, task_id, 'completed')
                with self._stats_lock:
                    self._drinks_processed += 1
                self._emit_event({
                    "type": "drink_detail_processed",
                    "task_id": task_id,
                    "url": url,
                    "metadata": metadata,
                    "percent": details.get("percent"),
                    "std_drinks": details.get("std_drinks"),
                    "drinks_processed": self._drinks_processed
                })
            else:
                print(f"[temp_scraper_debug] processing page task for url={url}")  # TODO: Remove this temp_scraper_debug print info.
                items, next_metadata = processor.get_items(url, metadata)
                if self.page_items_callback:
                    self.page_items_callback(len(items))
                self._emit_event({
                    "type": "page_items",
                    "task_id": task_id,
                    "url": url,
                    "count": len(items),
                })
                print(f"Found {len(items)} items.")

                processed_items = dbhandler(
                    conn,
                    items,
                    "u",
                    True,
                    start_index=self._drink_counter
                )
                self._drink_counter += processed_items

                # TODO: Reconsider this store-specific logic for building detail tasks. 
                #   Ideally, the processor should return any necessary detail tasks as part of get_items()
                #   or a separate method, rather than having controller logic that depends on retailer_name.
                if retailer_name == "ll":
                    detail_tasks = processor.build_detail_tasks(items)
                    for detail in detail_tasks:
                        url_detail = detail.get("url")
                        metadata_detail = detail.get("metadata")
                        if url_detail:
                            add_scrape_task(conn, retailer_name, url_detail, metadata_detail, run_id, task_type='drink_detail')

                if next_metadata:
                    next_url = next_metadata.get("next_url", url)
                    add_scrape_task(conn, retailer_name, next_url, next_metadata, run_id, task_type='page')
                    print(f"  - Discovered follow-up task: {next_url}")

                update_task_status(conn, task_id, 'completed')
                self._emit_event({
                    "type": "page_completed",
                    "task_id": task_id,
                    "url": url,
                    "count": len(items)
                })
            success = True
        except Exception as e:
            print(f"Error during task {task_id}: {e}")
            
            if (current_attempts + 1) >= self.max_retries:
                print(f"Task {task_id} exceeded max retries ({self.max_retries}). Marking as FAILED.")
                update_task_status(conn, task_id, 'failed', {"error": str(e)})
            else:
                update_task_status(conn, task_id, 'pending', {"error": str(e)})
        finally:
            conn.close()
            pending_now = self._count_pending_tasks(retailer_name, run_id)

        return {
            "task_type": task_type,
            "task_id": task_id,
            "url": url,
            "success": success,
            "pending": pending_now
        }

    def get_latest_run_id(self, retailer_name: str) -> str | None:
        """Return the most recent run id for a given retailer, if any."""
        if not retailer_name:
            return None
        conn = create_connection()
        if not conn:
            return None
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT uuid 
                FROM runs 
                WHERE retailer = ?
                ORDER BY start_time DESC 
                LIMIT 1
            """, (retailer_name.lower(),))
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def _worker_loop(self, worker_id: int, retailer_name: str, run_id: str | None):
        """Worker loop that continuously processes tasks."""
        print(f"[temp_scraper_debug] worker {worker_id} start run_id={run_id}")  # TODO: Remove this temp_scraper_debug print info.
        while not self._stop_workers:
            with self._stats_lock:
                if self._limit and self._page_tasks_completed >= self._limit:
                    break
            try:
                result = self.run_next(retailer_name, run_id)
            except Exception as e:
                print(f"Worker {worker_id} error: {e}")
                time.sleep(1)
                continue

            if not result:
                break

            task_type = result.get("task_type")
            with self._stats_lock:
                if task_type == 'drink_detail':
                    self._detail_tasks_completed += 1
                else:
                    self._page_tasks_completed += 1
                    if self._limit and self._page_tasks_completed >= self._limit:
                        self._stop_workers = True
                page_completed = self._page_tasks_completed
                detail_completed = self._detail_tasks_completed
                drinks_processed = self._drinks_processed

            self._emit_event({
                "type": "task_completed",
                "task_id": result.get("task_id"),
                "task_type": task_type,
                "success": result.get("success"),
                "url": result.get("url"),
                "pending": result.get("pending"),
                "page_completed": page_completed,
                "detail_completed": detail_completed,
                "drinks_processed": drinks_processed
            })

        print(f"[temp_scraper_debug] worker {worker_id} stopped")  # TODO: Remove this temp_scraper_debug print info.

    def _run_loop(self, retailer_name: str, num_workers: int):
        pending_start = self._count_pending_tasks(retailer_name, self._current_run_id)
        self._emit_event({
            "type": "run_started",
            "retailer": retailer_name,
            "run_id": self._current_run_id,
            "pending": pending_start,
            "limit": self._limit,
            "workers": num_workers
        })

        self._stop_workers = False
        workers = []
        for i in range(num_workers):
            t = threading.Thread(target=self._worker_loop, args=(i, retailer_name, self._current_run_id))
            t.daemon = True
            t.start()
            workers.append(t)

        for t in workers:
            t.join()

        pending_end = self._count_pending_tasks(retailer_name, self._current_run_id)
        with self._stats_lock:
            summary = {
                "type": "run_completed",
                "retailer": retailer_name,
                "run_id": self._current_run_id,
                "pages": self._page_tasks_completed,
                "details": self._detail_tasks_completed,
                "drinks": self._drinks_processed,
                "pending": pending_end,
                "limit": self._limit
            }
        self._emit_event(summary)

    def start_run(self, retailer_name: str, limit: int = None, run_id: str = None, num_workers: int = 1) -> queue.Queue:
        """Start a scraping run and return the event queue for progress consumption."""
        if self._run_thread and self._run_thread.is_alive():
            raise RuntimeError("A run is already in progress")

        self._limit = limit
        self._current_run_id = run_id
        self._active_retailer = retailer_name.lower()
        self._page_tasks_completed = 0
        self._detail_tasks_completed = 0
        self._drinks_processed = 0
        self._drink_counter = 0
        self._stop_workers = False
        self._event_queue = queue.Queue()
        self._run_thread = threading.Thread(target=self._run_loop, args=(retailer_name.lower(), num_workers))
        self._run_thread.daemon = True
        self._run_thread.start()
        return self._event_queue

    def run_parallel(self, num_workers: int = NUM_WORKERS, retailer: str = "ll", run_id: str = None, limit: int = None):
        queue = self.start_run(retailer, limit=limit, run_id=run_id, num_workers=num_workers)
        result = None
        while True:
            event = queue.get()
            if event["type"] == "run_completed":
                result = event
                break
        return result

    def _count_pending_tasks(self, retailer_name: str, run_id: str | None = None) -> int:
        """Count pending tasks for a retailer/run."""
        conn = create_connection()
        if not conn:
            return 0
        try:
            if run_id:
                return get_pending_tasks_count_by_run(conn, run_id, retailer_name)
            return get_pending_tasks_count(conn, retailer_name)
        finally:
            conn.close()

    def reset_in_progress_tasks(self, retailer_name: str | None = None, run_id: str | None = None) -> int:
        """
        Reset tasks that are marked as in_progress back to pending to allow restarting.
        """
        target_run_id = run_id or self._current_run_id
        target_retailer = retailer_name or self._active_retailer
        if not target_run_id and not target_retailer:
            return 0
        conn = create_connection()
        if not conn:
            return 0
        try:
            return reset_in_progress_tasks(
                conn,
                run_id=target_run_id,
                retailer=target_retailer
            )
        finally:
            conn.close()

    def process_all(self, retailer_name: str, limit: int = None, run_id: str = None):
        """Start a synchronous run and wait for completion."""
        queue = self.start_run(retailer_name, limit=limit, run_id=run_id, num_workers=1)
        while True:
            event = queue.get()
            if event["type"] == "run_completed":
                return event


if __name__ == "__main__":
    import argparse

    MAN_PAGE = """SCRAPING CONTROLLER(1)     CompareTheBrew     SCRAPING CONTROLLER(1)

NAME
       scraping.controller - Task-based scraping controller for CompareTheBrew

SYNOPSIS
       python3 -m scraping.controller RETAILER [OPTIONS]

DESCRIPTION
       Coordinates the scraping pipeline across different retailers using
       a Task Queue system with a progressive iterator approach.

ARGUMENTS
       RETAILER
              The retailer to scrape. Supported values:
              bws, ll (Liquorland), fc (First Choice)

OPTIONS
       -h, --help
              Display this man page.

       --discover
              Seeds the task queue with URLs to scrape. For pagination
              retailers (like BWS), it adds all pages at once.

       --run [RUN_ID]
              Process all pending tasks in the queue sequentially (optionally resume RUN_ID).
              If RUN_ID is omitted, the most recent run for RETAILER is resumed automatically.

       --next  Process only the next single task. Useful for debugging
              or controlled progression through the queue.

       --workers=N
              Number of worker threads for parallel processing.
              - Not specified or --workers=1: Sequential processing (default)
              - --workers=N where N > 1: Parallel processing with N workers

       --limit=N
              Limit the number of tasks to process. When combined with
              --discover, creates a new run and processes only N tasks.
              When used alone, resumes the most recent run and processes
              only N tasks. When used with --run or --run RUN_ID, overrides
              the default "process all" behavior for that run.

       --category=CAT
              Filter discovery by category. Supported values:
              beer, wine, spirits, premix

EXAMPLES
       # Full scrape with discovery and processing
       python3 -m scraping.controller bws --discover --run

       # Process one task at a time (iterator mode)
       python3 -m scraping.controller bws --next

       # Parallel processing with 8 workers
       python3 -m scraping.controller bws --discover --run --workers=8

       # Sequential processing (default)
       python3 -m scraping.controller bws --discover --run --workers=1

       # Discover only beer category
       python3 -m scraping.controller bws --discover --category=beer

       # Discover and process only first 3 pages
       python3 -m scraping.controller bws --discover --limit=3

       # Resume and process only next 2 pages
       python3 -m scraping.controller bws --limit=2

TASK TYPES
       page         Full page scraping (default)
       drink_detail Individual drink detail pages

RETRIERS
       Tasks are retried up to 3 times before being marked as failed.
       The max_retries setting can be adjusted in ScrapingController.

FILES
       scraping/sitemaps.json
              JSON file containing retailer URLs for discovery.

       scraping/SCRAPING_GUIDELINES.md
              Detailed guidelines for the scraping system.

SEE ALSO
       tools/task_queue_cli.py - View task queue status
       scraping/bws_processor.py - BWS-specific processor
       scraping/liquorland_processor.py - Liquorland processor

AUTHORS
       CompareTheBrew Team

CompareTheBrew                      2024           SCRAPING CONTROLLER(1)"""

    parser = argparse.ArgumentParser(
        description='Task-based Scraping Controller',
        usage='python3 -m scraping.controller RETAILER [OPTIONS]\n\nOptions:\n  --discover              Seed the queue\n  --run [RUN_ID]          Run all pending tasks (resume RUN_ID if provided)\n  --next                  Process only the next single task\n  --workers=N             Number of workers (>1 for parallel, 1 or omit for sequential)\n  --category=CAT          Filter by category (beer, wine, spirits, premix)\n  --limit=N               Limit tasks to process\n  -h, --help              Show this help message'
    )
    parser.add_argument('retailer', type=str, nargs='?', help='bws, ll, fc')
    parser.add_argument('--discover', action='store_true', help='Seed the queue')
    parser.add_argument('--run', nargs='?', const='__LATEST__', metavar='RUN_ID',
                        help='Process all pending tasks or resume a specific run when RUN_ID is provided')
    parser.add_argument('--next', action='store_true', help='Process only the next single task')
    parser.add_argument('--workers', type=int, default=1, help='Number of workers (1=sequential, >1=parallel)')
    parser.add_argument('--category', type=str, help='Filter discovery by category (beer, wine, spirits, premix)')
    parser.add_argument('--limit', type=int, help='Limit the number of tasks to process')
    
    import sys
    if '--help' in sys.argv or '-h' in sys.argv:
        print(MAN_PAGE)
        sys.exit(0)
    
    args = parser.parse_args()
    controller = ScrapingController()

    run_id = None
    if args.discover:
        run_id = controller.discover(args.retailer, category=args.category)

    run_target_id = None
    if args.run:
        if args.run == '__LATEST__':
            run_target_id = run_id or controller.get_latest_run_id(args.retailer)
            if not run_target_id:
                print("Warning: no previous run found; processing whatever tasks are available.")
        else:
            run_target_id = args.run
    elif run_id:
        run_target_id = run_id

    limit = args.limit
    if args.next:
        limit = 1

    workers = args.workers if args.workers and args.workers > 0 else 1
    result = controller.run_parallel(num_workers=workers, retailer=args.retailer, run_id=run_target_id, limit=limit)
    if result:
        print(f"Run {result.get('run_id')} completed: {result.get('pages', 0)} pages, "
              f"{result.get('details', 0)} details, pending {result.get('pending', 0)}")
