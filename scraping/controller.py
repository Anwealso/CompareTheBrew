import json
import os
import uuid
import threading
import queue
import time
from datetime import datetime
from typing import Dict, List
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from scraping.processor import RetailerProcessor
from scraping.bws_processor import BWSProcessor
from scraping.liquorland_processor import LiquorlandProcessor
from db.databaseHandler import (
    create_connection, upsert_source, dbhandler, 
    add_scrape_task, get_next_pending_task, get_next_pending_task_by_run,
    update_task_status, get_pending_tasks_count, get_pending_tasks_count_by_run,
    increment_task_attempts, create_run, update_drink_details
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

    def _load_sitemaps(self) -> Dict[str, List[str]]:
        """
        Loads the sitemaps configuration from disk.
        """
        if not os.path.exists(self.sitemaps_file):
            print(f"Sitemaps file {self.sitemaps_file} not found. Using default structure.")
            return {"bws": [], "ll": [], "fc": []}
        
        with open(self.sitemaps_file, 'r') as f:
            return json.load(f)

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

    def run_next(self, retailer_name: str, run_id: str = None) -> bool:
        """
        Progressive execution: Pulls ONE task from the queue, processes it.
        
        Args:
            retailer_name: The retailer to process tasks for
            run_id: Optional run_id to filter tasks
        """
        print(f"[temp_scraper_debug] enter ScrapingController.run_next(retailer={retailer_name}, run_id={run_id})")  # TODO: Remove this temp_scraper_debug print info.
        retailer_name = retailer_name.lower()
        conn = create_connection()
        if not conn:
            return False

        if run_id:
            task = get_next_pending_task_by_run(conn, run_id, retailer_name)
        else:
            task = get_next_pending_task(conn, retailer_name)
        
        if not task:
            self.last_task_type = None
            conn.close()
            return False

        task_id = task[0]
        url = task[2]
        status = task[3]
        metadata_str = task[4]
        run_id = task[5] if len(task) > 5 else None
        current_attempts = task[6] if len(task) > 6 else 0
        task_type = task[9] if len(task) > 9 else 'page'
        self.last_task_type = task_type
        
        metadata = json.loads(metadata_str) if metadata_str else {}
        processor = self.processors[retailer_name]
        processor.progress_callback = self.progress_callback

        print(f"Processing Task {task_id} (Type: {task_type}) (Attempt {current_attempts + 1}/{self.max_retries}): {url}")
        
        self.current_url = url
        
        increment_task_attempts(conn, task_id)
        update_task_status(conn, task_id, 'in_progress')
        
        now = datetime.now().isoformat()
        upsert_source(conn, url, retailer_name, now)
        
        try:
            if task_type == 'drink_detail':
                print(f"[temp_scraper_debug] processing drink_detail task for url={url}")  # TODO: Remove this temp_scraper_debug print info.
                details = processor.process_drink_detail(url, metadata)
                print(f"Got details: percent={details.get('percent')}, std_drinks={details.get('std_drinks')}")
                if metadata:
                    store = metadata.get("store", retailer_name)
                    link = metadata.get("link")
                    percent = details.get("percent", 0.0)
                    std_drinks = details.get("std_drinks", 0.0)
                    if link:
                        update_drink_details(conn, store, link, percent, std_drinks)
                update_task_status(conn, task_id, 'completed')
            else:
                print(f"[temp_scraper_debug] processing page task for url={url}")  # TODO: Remove this temp_scraper_debug print info.
                items, next_metadata = processor.get_items(url, metadata)
                if self.page_items_callback:
                    self.page_items_callback(len(items))
                print(f"Found {len(items)} items.")

                processed_items = dbhandler(
                    conn,
                    items,
                    "u",
                    True,
                    item_callback=self.drink_callback,
                    start_index=self._drink_counter
                )
                self._drink_counter += processed_items

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
        except Exception as e:
            print(f"Error during task {task_id}: {e}")
            
            if (current_attempts + 1) >= self.max_retries:
                print(f"Task {task_id} exceeded max retries ({self.max_retries}). Marking as FAILED.")
                update_task_status(conn, task_id, 'failed', {"error": str(e)})
            else:
                update_task_status(conn, task_id, 'pending', {"error": str(e)})
            
        conn.close()
        return True

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

    def _worker_thread(self, worker_id: int, task_queue: queue.Queue, run_id: str = None):
        """Worker thread that processes tasks from the queue."""
        print(f"[temp_scraper_debug] worker {worker_id} start run_id={run_id}")  # TODO: Remove this temp_scraper_debug print info.
        print(f"Worker {worker_id} started")
        
        while not self._stop_workers:
            try:
                result = self.run_next("ll", run_id)
                if not result:
                    time.sleep(0.5)
            except Exception as e:
                print(f"Worker {worker_id} error: {e}")
                time.sleep(1)
        
        print(f"Worker {worker_id} stopped")

    def run_parallel(self, num_workers: int = NUM_WORKERS, retailer: str = "ll", run_id: str = None):
        """
        Run tasks in parallel using multiple worker threads.
        
        Args:
            num_workers: Number of worker threads
            retailer: Retailer to process
            run_id: Optional run_id to filter tasks
        """
        self._stop_workers = False
        workers = []
        
        print(f"[temp_scraper_debug] enter run_parallel(num_workers={num_workers}, retailer={retailer}, run_id={run_id})")  # TODO: Remove this temp_scraper_debug print info.
        print(f"Starting {num_workers} workers...")
        
        for i in range(num_workers):
            t = threading.Thread(target=self._worker_thread, args=(i, None, run_id))
            t.daemon = True
            t.start()
            workers.append(t)
        
        try:
            while True:
                time.sleep(2)
                conn = create_connection()
                if conn:
                    pending = get_pending_tasks_count(conn, retailer)
                    conn.close()
                    if pending == 0:
                        print("No more pending tasks, stopping workers...")
                        break
        except KeyboardInterrupt:
            print("\nStopping workers...")
        
        self._stop_workers = True
        for t in workers:
            t.join(timeout=2)
        
        print("All workers stopped")

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

    def process_all(self, retailer_name: str, limit: int = None, run_id: str = None):
        """Looping run_next until exhaustion or limit reached.
        
        Args:
            retailer_name: The retailer to process
            limit: Optional max number of tasks to process
            run_id: Optional run_id to filter tasks
        """
        console = Console()
        pending_start = self._count_pending_tasks(retailer_name, run_id)
        target_task_total = limit if limit else max(pending_start, 1)
        page_completed = 0
        detail_completed = 0
        drinks_expected = 0
        drinks_processed = 0
        self._drink_counter = 0

        console.print(f"[temp_scraper_debug] starting process_all for {retailer_name} (limit={limit})")  # TODO: Remove this temp_scraper_debug print info.
        console.print(f"[temp_scraper_debug] initial pending tasks: {pending_start}, target total: {target_task_total}")  # TODO: Remove this temp_scraper_debug print info.

        def on_page_items(count: int):
            nonlocal drinks_expected
            drinks_expected += count
            progress.update(drink_task, total=max(drinks_expected, drinks_processed))

        def on_drink(drink, absolute_index, page_total, inserted):
            nonlocal drinks_processed
            drinks_processed += 1
            total_label = max(drinks_expected, page_total or absolute_index)
            status = "inserted" if inserted else "updated"
            progress.update(drink_task, completed=drinks_processed, description=f"[cyan]{drink.brand} {drink.name}")
            price_value = float(drink.price) if drink.price is not None else 0.0
            console.print(
                f"[dim]Drink {absolute_index}/{total_label} ({status}):[/dim] "
                f"{drink.brand} {drink.name} | "
                f"ABV {float(drink.percent or 0.0):.2f}% | "
                f"StdDrinks {float(drink.stdDrinks or 0.0):.2f} | "
                f"Price ${price_value:.2f}"
            )  # TODO: Remove this temp_scraper_debug print info.

        self.page_items_callback = on_page_items
        self.drink_callback = on_drink

        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console
            ) as progress:
                page_task = progress.add_task(
                    description=f"[green]{retailer_name} Pages | [cyan]Pending {pending_start}",
                    total=target_task_total
                )
                drink_task = progress.add_task(
                    description=f"[cyan]{retailer_name} Drinks | Awaiting drinks...",
                    total=0
                )

                while True:
                    if limit and page_completed >= limit:
                        progress.update(page_task, description=f"[yellow]{retailer_name}: Limit reached ({limit})")
                        break

                    result = self.run_next(retailer_name, run_id)
                    if not result:
                        pending_now = self._count_pending_tasks(retailer_name, run_id)
                        final_status = "[green]Completed - no more tasks" if pending_now == 0 else "[red]Stopped - no more pending tasks"
                        progress.update(page_task, description=final_status)
                        break

                    task_type = self.last_task_type
                    if task_type == 'drink_detail':
                        detail_completed += 1
                    else:
                        page_completed += 1

                    pending_now = self._count_pending_tasks(retailer_name, run_id)
                    total_goal = target_task_total if limit else max(page_completed + pending_now, target_task_total)
                    desc = (
                        f"[green]{retailer_name}: Tasks {page_completed}/{total_goal} | "
                        f"Pending {pending_now}"
                    )
                    if limit:
                        desc += f" (limit {limit})"
                    if detail_completed:
                        desc += f" | Detail updates: {detail_completed}"

                    progress.update(
                        page_task,
                        completed=page_completed,
                        total=max(total_goal, target_task_total),
                        description=desc
                    )
        finally:
            self.page_items_callback = None
            self.drink_callback = None

        console.print(f"[temp_scraper_debug] finished process_all: {page_completed} page tasks, {detail_completed} detail tasks, {drinks_processed} drinks processed")  # TODO: Remove this temp_scraper_debug print info.


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
    
    if args.discover:
        run_id = controller.discover(args.retailer, category=args.category)
    else:
        run_id = None

    run_target_id = None
    if args.run:
        if args.run == '__LATEST__':
            run_target_id = run_id or controller.get_latest_run_id(args.retailer)
            if not run_target_id:
                print("Warning: no previous run found; processing whatever tasks are available.")
        else:
            run_target_id = args.run

    if args.workers > 1:
        controller.run_parallel(num_workers=args.workers, retailer=args.retailer, run_id=run_target_id or run_id)
    elif args.run:
        controller.process_all(args.retailer, limit=args.limit, run_id=run_target_id)
    elif args.next:
        controller.run_next(args.retailer, run_id=run_id)
    elif args.limit:
        controller.process_all(args.retailer, limit=args.limit, run_id=run_id)
    elif not args.discover:
        conn = create_connection()
        count = get_pending_tasks_count(conn, args.retailer)
        conn.close()
        
        if count == 0:
            run_id = controller.discover(args.retailer, category=args.category)
        controller.process_all(args.retailer)
