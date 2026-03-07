import json
import os
from datetime import datetime
from typing import Dict, List, Optional
from scraping.processor import RetailerProcessor
from scraping.bws_processor import BWSProcessor
from scraping.coles_processor import LiquorlandProcessor, FirstChoiceProcessor
from db.databaseHandler import (
    create_connection, upsert_source, dbhandler, 
    add_scrape_task, get_next_pending_task, update_task_status, get_pending_tasks_count,
    increment_task_attempts
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
        self.processors: Dict[str, RetailerProcessor] = {
            "bws": BWSProcessor(),
            "ll": LiquorlandProcessor(),
            "fc": FirstChoiceProcessor()
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

    def discover(self, retailer_name: str):
        """
        Discovery Phase: Hits the retailer's seed URLs once to seed the task queue.
        For pagination retailers (like BWS), it adds all pages at once.
        For others, it adds the first page and depends on the processor to find 'next'.
        """
        retailer_name = retailer_name.lower()
        if retailer_name not in self.processors:
            print(f"No processor found for retailer: {retailer_name}")
            return

        processor = self.processors[retailer_name]
        seed_urls = self.sitemaps.get(retailer_name, [])
        conn = create_connection()
        if not conn:
            return

        print(f"Starting discovery for {retailer_name}...")
        for url in seed_urls:
            print(f"Discovering from: {url}")
            tasks = processor.discover_tasks(url)
            print(f"Found {len(tasks)} tasks.")
            for t in tasks:
                add_scrape_task(conn, retailer_name, t["url"], t["metadata"])
                print(f"  - Queued task: {t['url']}")
        
        conn.close()

    def run_next(self, retailer_name: str) -> bool:
        """
        Progressive execution: Pulls ONE task from the queue, processes it, 
        and handles potentially returned 'next' metadata.
        """
        retailer_name = retailer_name.lower()
        conn = create_connection()
        if not conn:
            return False

        task = get_next_pending_task(conn, retailer_name)
        if not task:
            # print(f"No pending tasks for {retailer_name}")
            conn.close()
            return False

        # IDs in SQLite are usually 0-indexed in the cursor result if using SELECT *
        # ID, retailer, url, status, metadata, attempts, created_at, updated_at
        task_id = task[0]
        url = task[2]
        metadata_str = task[4]
        current_attempts = task[5] if task[5] is not None else 0
        
        metadata = json.loads(metadata_str) if metadata_str else {}
        processor = self.processors[retailer_name]
        
        print(f"Processing Task {task_id} (Attempt {current_attempts + 1}/{self.max_retries}): {url}")
        
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
