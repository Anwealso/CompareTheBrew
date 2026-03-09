import json
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple
from scripts.classItem import Item
from config import Config
from scraping.fetcher import Fetcher, get_fetcher
    
class RetailerProcessor(ABC):
    """
    Base class for retailer-specific scraping logic.
    Provides shared methods for fetching URL content and defines the interface
    for extracting items.
    """
    def __init__(self):
        self.api_key: str = Config.SCRAPINGBEE_API_KEY
        self.fetcher: Fetcher = get_fetcher()
        self.progress_callback = None

    def clean_numeric(self, val: any) -> float:
        """Cleans a string/number to a float."""
        if val is None: return 0.0
        try:
            # Remove symbols and handle string conversion
            s = str(val).replace('%', '').replace('$', '').replace(',', '').strip()
            # Handle "Approx. 1.5"
            if ' ' in s: s = s.split(' ')[0]
            return float(s)
        except (ValueError, TypeError):
            return 0.0

    def parse_volume(self, name: str) -> float:
        """Parses volume in ml from a product name."""
        import re
        # Match "750ml", "750 mL", "1L", "2.25 L" etc.
        match = re.search(r'(\d+(?:\.\d+)?)\s*(ml|l)', name.lower())
        if match:
            val, unit = match.groups()
            try:
                vol = float(val)
                if unit == 'l': vol *= 1000
                return vol
            except: pass
        return 0.0

    def fetch_url(self, url: str) -> Optional[str]:
        """
        Fetches the content of a URL.
        """
        return self.fetcher.fetch_url(url)
    
    @abstractmethod
    def get_items(self, url: str, metadata: Optional[dict] = None) -> Tuple[List[Item], Optional[dict]]:
        """
        Extracts items from the given URL.
        """
        pass

    @abstractmethod
    def discover_tasks(self, url: str) -> List[dict]:
        """
        Initial discovery of tasks for a given seed URL.
        """
        pass
