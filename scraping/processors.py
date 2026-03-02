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
        
        Args:
            url (str): The URL to scrape.
            metadata (Optional[dict]): Task metadata (e.g., page number, session state).
            
        Returns:
            Tuple[List[Item], Optional[dict]]: List of extracted items and metadata for the next task if applicable.
        """
        pass

    @abstractmethod
    def discover_tasks(self, url: str) -> List[dict]:
        """
        Initial discovery of tasks for a given seed URL.
        
        Returns:
            List[dict]: A list of task definitions (url, metadata).
        """
        pass

class BWSProcessor(RetailerProcessor):
    """
    Processor for BWS (Beer Wine Spirits).
    """
    def discover_tasks(self, url: str) -> List[dict]:
        """
        BWS discovery: Find total pages and create all page tasks immediately.
        """
        tasks = []
        discovery_url = url.replace("pageSize=1000", "pageSize=1")
        content = self.fetch_url(discovery_url)
        if not content:
            return [{"url": url, "metadata": {"page": 1}}]

        try:
            data = json.loads(content)
            total_count = data.get('TotalProductCount', 0)
            page_size = 1000
            num_pages = (total_count + page_size - 1) // page_size
            
            for page in range(1, num_pages + 1):
                page_url = url.replace("pageNumber=1", f"pageNumber={page}")
                tasks.append({"url": page_url, "metadata": {"page": page}})
        except Exception as e:
            print(f"Error in BWS discovery: {e}")
            tasks.append({"url": url, "metadata": {"page": 1}})
            
        return tasks

    def get_items(self, url: str, metadata: Optional[dict] = None) -> Tuple[List[Item], Optional[dict]]:
        result = list()
        content = self.fetch_url(url)
        if not content:
            return result, None
            
        try:
            data = json.loads(content)
        except Exception as e:
            print(f"Error parsing JSON from BWS URL {url}: {e}")
            return result, None
        
        if 'Bundles' not in data:
            return result, None
            
        bundles = data['Bundles']
        for drink in bundles:
            products = drink.get('Products', [])
            for subdrink in products:
                # ... Simplified extraction for brevity, keeping the core logic from previous turns ...
                parentcode = "None"
                item_numb = 1.0
                percent_alcohol = 0.0
                image_numb = "None"
                std_drinks = -1.0
                link = "None"
                style = "None"
                size = 0.0
                
                for i in subdrink.get("AdditionalDetails", []):
                    if i["Name"] == "parentstockcode":
                        parentcode = i["Value"]
                    elif i["Name"] == "productunitquantity":
                        try: item_numb = float(i["Value"])
                        except: item_numb = 1.0
                    elif i["Name"] == "alcohol%":
                        try: percent_alcohol = float(i["Value"].replace('%',''))
                        except: percent_alcohol = 0.0
                    elif i["Name"] == "image1":
                        image_numb = i["Value"]
                    elif i["Name"] == "standarddrinks":
                        try: std_drinks = float(i["Value"].replace('Approx.','').replace('Approx','').split(' ')[0])
                        except: std_drinks = -1.0
                    elif i["Name"] == "bwsproducturl":
                        link = i["Value"]
                    elif i["Name"] == "standardcategory":
                        style = i["Value"]
                    elif i["Name"] == "liquorsize":
                        sz = i["Value"]
                        if "Pack" in sz: 
                            try: sz = sz.split(" ")[2]
                            except: sz = "0"
                        sz = sz.replace("ml", "").replace("mL", "")
                        if "L" in sz:
                            try: size = float(sz.split("L")[0]) * 1000
                            except: size = 0.0
                        else:
                            try: size = float(sz)
                            except: size = 0.0
                        
                drink_link = f"https://bws.com.au/product/{parentcode}/{link}"
                image_link = f"https://edgmedia.bws.com.au/bws/media/products/{image_numb}"
                
                price = float(subdrink.get("Price", 0))
                efficiency = (std_drinks * item_numb / price) if price > 0 and std_drinks > 0 else 0.0
                
                item = Item(store="bws", brand=subdrink["BrandName"], name=subdrink["Name"].strip(), 
                            type=style, price=price, link=drink_link, ml=size, percent=percent_alcohol,
                            std_drinks=std_drinks, numb_items=item_numb, efficiency=efficiency, image=image_link,
                            promotion=subdrink.get('IsOnSpecial', False), old_price=subdrink.get("WasPrice", 0))
                result.append(item)
                
        # BWS doesn't need to return a next_task here because all tasks were pre-discovered
        return result, None

class LiquorlandProcessor(RetailerProcessor):
    """
    Processor for Liquorland (Hybrid approach example).
    """
    def discover_tasks(self, url: str) -> List[dict]:
        # Only discover the first page
        return [{"url": url, "metadata": {"page": 1}}]

    def get_items(self, url: str, metadata: Optional[dict] = None) -> Tuple[List[Item], Optional[dict]]:
        print(f"Liquorland: scraping {url}")
        # Dummy logic: simulate finding a 'next' page
        page = metadata.get("page", 1) if metadata else 1
        items = [Item(store="ll", brand="LL", name=f"LL {page}-{i}", type="Beer", price=20, link=url, ml=375, percent=4.5, std_drinks=1.3, numb_items=6, efficiency=0.39, image="", promotion=False, old_price=20) for i in range(3)]
        
        next_metadata = None
        if page < 3: # Simulate only 3 pages
            next_metadata = {"page": page + 1, "next_url": f"{url}?page={page+1}"}
            
        return items, next_metadata

class FirstChoiceProcessor(RetailerProcessor):
    """
    Processor for First Choice (Hybrid approach example).
    """
    def discover_tasks(self, url: str) -> List[dict]:
        return [{"url": url, "metadata": {"page": 1}}]

    def get_items(self, url: str, metadata: Optional[dict] = None) -> Tuple[List[Item], Optional[dict]]:
        print(f"FirstChoice: scraping {url}")
        page = metadata.get("page", 1) if metadata else 1
        items = [Item(store="fc", brand="FC", name=f"FC {page}-{i}", type="Wine", price=15, link=url, ml=750, percent=13, std_drinks=7.7, numb_items=1, efficiency=0.51, image="", promotion=True, old_price=18) for i in range(3)]
        
        next_metadata = None
        if page < 2: # Simulate only 2 pages
            next_metadata = {"page": page + 1, "next_url": f"{url}?page={page+1}"}
            
        return items, next_metadata
