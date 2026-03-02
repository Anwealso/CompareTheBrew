import json
import os
from datetime import datetime
from typing import Dict, List, Type
from scrapers.processors import RetailerProcessor, BWSProcessor, LiquorlandProcessor, FirstChoiceProcessor
from scripts.databaseHandler import create_connection, upsert_source, dbhandler

class ScrapingManager:
    def __init__(self, sitemaps_file: str = "sitemaps.json"):
        self.sitemaps_file = sitemaps_file
        self.processors: Dict[str, RetailerProcessor] = {
            "bws": BWSProcessor(),
            "ll": LiquorlandProcessor(),
            "fc": FirstChoiceProcessor()
        }
        self.sitemaps = self._load_sitemaps()

    def _load_sitemaps(self) -> Dict[str, List[str]]:
        if not os.path.exists(self.sitemaps_file):
            print(f"Sitemaps file {self.sitemaps_file} not found. Using default structure.")
            return {"bws": [], "ll": [], "fc": []}
        
        with open(self.sitemaps_file, 'r') as f:
            return json.load(f)

    def scrape_retailer(self, retailer_name: str):
        retailer_name = retailer_name.lower()
        if retailer_name not in self.processors:
            print(f"No processor found for retailer: {retailer_name}")
            return

        processor = self.processors[retailer_name]
        urls = self.sitemaps.get(retailer_name, [])

        if not urls:
            print(f"No URLs found for retailer: {retailer_name}")
            return

        conn = create_connection()
        if not conn:
            print("Failed to connect to database.")
            return

        all_items = []
        now = datetime.now().isoformat()

        for url in urls:
            print(f"Scraping URL: {url} for retailer: {retailer_name}")
            
            # Update Sources table
            upsert_source(conn, url, retailer_name, now)
            
            # Extract items using injected dependency
            items = processor.get_items(url)
            print(f"Found {len(items)} items on page.")
            all_items.extend(items)

        # Write data to database
        if all_items:
            # dbhandler(conn, list, mode, populate)
            # mode "u" updates existing and adds new if populate=True
            dbhandler(conn, all_items, "u", True)
            print(f"Successfully processed {len(all_items)} items for {retailer_name}")
        else:
            print(f"No items found for {retailer_name}")

        conn.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python scrapers/manager.py <retailer_name>")
        sys.exit(1)
    
    manager = ScrapingManager()
    manager.scrape_retailer(sys.argv[1])
