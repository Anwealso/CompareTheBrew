import json
import os
import uuid
from datetime import datetime
from typing import Dict, List, Optional
from scraping.processor import RetailerProcessor
from scraping.bws_processor import BWSProcessor
from scraping.liquorland_processor import LiquorlandProcessor
from db.databaseHandler import (
    create_connection, upsert_source, dbhandler, 
    add_scrape_task, get_next_pending_task, get_next_pending_task_by_run,
    update_task_status, get_pending_tasks_count,
    increment_task_attempts, create_run, update_run_completed
)

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
        self.sitemaps_file = sitemaps_file
        self.max_retries = max_retries
        self.progress_callback = None
        self.current_url = ""
        self.processors: Dict[str, RetailerProcessor] = {
            "bws": BWSProcessor(),
            "ll": LiquorlandProcessor(),
        }
        self.sitemaps = self._load_sitemaps()

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
            # Narrow down to just the urls for that category if specified
            seed_urls = [url for url in seed_urls if category in url.lower()]
        
        conn = create_connection()
        if not conn:
            return

        # Create the run entry in the database
        create_run(conn, run_id, retailer=retailer_name, category=category)
        
        print(f"Starting discovery for {retailer_name}" + (f" ({category})" if category else "") + f" (run_id: {run_id})...")
        for url in seed_urls:
            print(f"Discovering from: {url}")
            tasks = processor.discover_tasks(url)
            print(f"Found {len(tasks)} tasks.")
            for t in tasks:
                add_scrape_task(conn, retailer_name, t["url"], t["metadata"], run_id)
                print(f"  - Queued task: {t['url']}")
        
        conn.close()
        return run_id

    def run_next(self, retailer_name: str, run_id: str = None) -> bool:
        """
        Progressive execution: Pulls ONE task from the queue, processes it, 
        and handles potentially returned 'next' metadata.
        
        Args:
            retailer_name: The retailer to process tasks for
            run_id: Optional run_id to filter tasks. If provided, will优先 select
                    in_progress tasks for this run first, then pending ones.
        """
        retailer_name = retailer_name.lower()
        conn = create_connection()
        if not conn:
            return False

        # Get next task - either by run_id (if provided) or just retailer
        if run_id:
            task = get_next_pending_task_by_run(conn, run_id, retailer_name)
        else:
            task = get_next_pending_task(conn, retailer_name)
        
        if not task:
            # print(f"No pending tasks for {retailer_name}")
            conn.close()
            return False

        # IDs in SQLite are usually 0-indexed in the cursor result if using SELECT *
        # ID, retailer, url, status, metadata, run_id, attempts, created_at, updated_at
        task_id = task[0]
        url = task[2]
        metadata_str = task[4]
        current_attempts = task[6] if task[6] is not None else 0
        
        metadata = json.loads(metadata_str) if metadata_str else {}
        processor = self.processors[retailer_name]
        processor.progress_callback = self.progress_callback
        
        print(f"Processing Task {task_id} (Attempt {current_attempts + 1}/{self.max_retries}): {url}")
        
        self.current_url = url
        
        # Increment attempts and set status to in_progress
        increment_task_attempts(conn, task_id)
        update_task_status(conn, task_id, 'in_progress')
        
        now = datetime.now().isoformat()
        # Track that we've hit this URL
        upsert_source(conn, url, retailer_name, now)
        
        try:
            items, next_metadata = processor.get_items(url, metadata)
            print(f"Found {len(items)} items.")
            
            if items:
                dbhandler(conn, items, "u", True)
                
            # Hybrid Approach: If the processor found a 'next page' (e.g., session-based sites)
            if next_metadata:
                next_url = next_metadata.get("next_url", url)
                add_scrape_task(conn, retailer_name, next_url, next_metadata)
                print(f"  - Discovered follow-up task: {next_url}")
                
            update_task_status(conn, task_id, 'completed')
        except Exception as e:
            print(f"Error during task {task_id}: {e}")
            
            # If we've reached max retries, mark as permanently failed
            if (current_attempts + 1) >= self.max_retries:
                print(f"Task {task_id} exceeded max retries ({self.max_retries}). Marking as FAILED.")
                update_task_status(conn, task_id, 'failed', {"error": str(e)})
            else:
                # Move to back of queue by updating its status to pending 
                # (DBHandler.update_task_status handles moving it to the back)
                update_task_status(conn, task_id, 'pending', {"error": str(e)})
            
        conn.close()
        return True

    def process_all(self, retailer_name: str):
        """Looping run_next until exhaustion."""
        print(f"Processing all tasks for {retailer_name}...")
        while self.run_next(retailer_name):
            pass

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Task-based Scraping Controller')
    parser.add_argument('retailer', type=str, help='bws, ll, fc')
    parser.add_argument('--discover', action='store_true', help='Seed the queue')
    parser.add_argument('--run', action='store_true', help='Run all pending tasks')
    parser.add_argument('--next', action='store_true', help='Process only the next single task')
    
    args = parser.parse_args()
    controller = ScrapingController()
    
    if args.discover:
        controller.discover(args.retailer)
    
    if args.run:
        controller.process_all(args.retailer)
    elif args.next:
        controller.run_next(args.retailer)
    elif not args.discover:
        # Default behavior if no flag: Discover if queue is empty, then run all
        conn = create_connection()
        count = get_pending_tasks_count(conn, args.retailer)
        conn.close()
        
        if count == 0:
            controller.discover(args.retailer)
        controller.process_all(args.retailer)
