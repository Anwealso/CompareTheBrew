import json
import os
from datetime import datetime
from typing import Dict, List, Type
from scrapers.processors import RetailerProcessor, BWSProcessor, LiquorlandProcessor, FirstChoiceProcessor
from scripts.databaseHandler import create_connection, upsert_source, dbhandler

class ScrapingManager:
    """
    Central manager for coordinating the scraping pipeline across different retailers.
    
    This class handles the following steps:
    1. Loads retailer "sitemaps" (list of URLs to scrape) from a JSON file.
    2. Initializes and injects the appropriate Processor for each retailer.
    3. Iterates through the URLs for a given retailer, extracts items, 
       and logs the scraping event in the database.
    4. Saves the extracted item data into the central database.
    """
    def __init__(self, sitemaps_file: str = "sitemaps.json"):
        """
        Initializes the manager with a sitemaps file.
        
        Args:
            sitemaps_file (str): Path to the JSON file containing retailer URLs.
        """
        self.sitemaps_file = sitemaps_file
        self.processors: Dict[str, RetailerProcessor] = {
            "bws": BWSProcessor(),
            "ll": LiquorlandProcessor(),
            "fc": FirstChoiceProcessor()
        }
        self.sitemaps = self._load_sitemaps()

    def _load_sitemaps(self) -> Dict[str, List[str]]:
        """
        Loads the sitemaps configuration from disk.
        
        Returns:
            Dict[str, List[str]]: A dictionary mapping retailer keys to lists of URLs.
        """
        if not os.path.exists(self.sitemaps_file):
            print(f"Sitemaps file {self.sitemaps_file} not found. Using default structure.")
            return {"bws": [], "ll": [], "fc": []}
        
        with open(self.sitemaps_file, 'r') as f:
            return json.load(f)

    def scrape_retailer(self, retailer_name: str):
        """
        Executes the full scraping pipeline for a single retailer.
        
        Args:
            retailer_name (str): The name or key of the retailer to scrape 
                                  (e.g., 'bws', 'll', 'fc').
        """
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
