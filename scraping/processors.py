import json
import ssl
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple
from urllib.request import Request, urlopen
from scripts.classItem import Item
from config import Config

class RetailerProcessor(ABC):
    """
    Base class for retailer-specific scraping logic.
    Provides shared methods for fetching URL content and defines the interface
    for extracting items.
    """
    def __init__(self):
        self.api_key = Config.SCRAPING_API_KEY

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
        if self.api_key:
            import urllib.parse
            encoded_url = urllib.parse.quote(url)
            sb_url = f"https://app.scrapingbee.com/api/v1/?api_key={self.api_key}&url={encoded_url}&render_js=true&premium_proxy=true&country_code=au"
            try:
                context = ssl._create_unverified_context()
                with urlopen(sb_url, context=context) as response:
                    return response.read().decode('utf-8')
            except Exception as e:
                print(f"ScrapingBee error for {url}: {e}")
        
        try:
            context = ssl._create_unverified_context()
            req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urlopen(req, context=context) as response:
                return response.read().decode('utf-8')
        except Exception as e:
            print(f"Direct fetch error for {url}: {e}")
            return None

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
