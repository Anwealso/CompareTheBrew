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

class BWSProcessor(RetailerProcessor):
    """
    Processor for BWS (Beer Wine Spirits).
    """
    def discover_tasks(self, url: str) -> List[dict]:
        """
        BWS discovery: Determines total pagination depth and seeds the queue.
        
        This method hits the BWS API with a minimal page size to retrieve the 
        'TotalProductCount'. It then calculates the number of 1000-item pages 
        required and returns a list of specific page URLs. If the API reports 0 products or the fetch fails, it returns 
        at least the seed URL as a single task to prevent silent skips.
        """
        tasks = []
        discovery_url = url.replace("pageSize=1000", "pageSize=1")
        content = self.fetch_url(discovery_url)
        
        # Fallback: if fetch fails, queue the original seed URL to try later
        if not content:
            return [{"url": url, "metadata": {"page": 1}}]

        try:
            data = json.loads(content)
            total_count = data.get('TotalProductCount', 0)
            page_size = 1000
            num_pages = (total_count + page_size - 1) // page_size
            
            # Ensure at least one page task is created even if total_count is 0
            if num_pages == 0:
                num_pages = 1
                
            for page in range(1, num_pages + 1):
                page_url = url.replace("pageNumber=1", f"pageNumber={page}")
                tasks.append({"url": page_url, "metadata": {"page": page}})
        except Exception as e:
            print(f"Error in BWS discovery: {e}")
            # Fallback on parse error
            tasks.append({"url": url, "metadata": {"page": 1}})
            
        return tasks

    def get_items(self, url: str, metadata: Optional[dict] = None) -> Tuple[List[Item], Optional[dict]]:
        """
        Parses BWS JSON data to extract drinks.
        
        Implements robust extraction that validates the existence of 
        nested keys and handles numeric conversion/cleaning (e.g. stripping '%' 
        from ABV or 'Approx' from standard drinks) to prevent NoneType or 
        ValueError crashes on incomplete product data.
        """
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
                # Robust extraction logic with defaults
                parentcode = "None"
                item_numb = 1.0
                percent_alcohol = 0.0
                image_numb = "None"
                std_drinks = 0.0
                link = "None"
                style = "None"
                size = 0.0
                
                # Iterate through BWS additional details to populate item properties
                for i in subdrink.get("AdditionalDetails", []):
                    name = i.get("Name")
                    val = i.get("Value")
                    if not val: continue
                    
                    if name == "parentstockcode":
                        parentcode = val
                    elif name == "productunitquantity":
                        try: item_numb = float(val)
                        except: item_numb = 1.0
                    elif name == "alcohol%":
                        # Strip '%' and convert to float
                        try: percent_alcohol = float(str(val).replace('%','').strip())
                        except: percent_alcohol = 0.0
                    elif name == "image1":
                        image_numb = val
                    elif name == "standarddrinks":
                        # Handle 'Approx.' prefix and trailing text
                        try: 
                            std_val = str(val).replace('Approx.','').replace('Approx','').strip().split(' ')[0]
                            std_drinks = float(std_val)
                        except: std_drinks = 0.0
                    elif name == "bwsproducturl":
                        link = val
                    elif name == "standardcategory":
                        style = val
                    elif name == "liquorsize":
                        # Parse size, handling 'Pack of X' and 'L' vs 'ml' units
                        sz = str(val).lower()
                        if "pack" in sz: 
                            try: sz = sz.split(" ")[2]
                            except: sz = "0"
                        sz = sz.replace("ml", "").replace("l", "")
                        try:
                            size = float(sz)
                            if "l" in str(val).lower() and "ml" not in str(val).lower():
                                size *= 1000 # Convert Liters to milliliters
                        except: size = 0.0
                        
                drink_link = f"https://bws.com.au/product/{parentcode}/{link}"
                image_link = f"https://edgmedia.bws.com.au/bws/media/products/{image_numb}"
                
                # Safe price conversion
                price = 0.0
                try:
                    p_val = subdrink.get("Price")
                    if p_val is not None:
                        price = float(p_val)
                except: price = 0.0
                
                # Calculate efficiency (standard drinks per dollar)
                efficiency = (std_drinks * item_numb / price) if price > 0 and std_drinks > 0 else 0.0
                
                # Map BWS fields to the common Item class
                item = Item(store="bws", brand=subdrink.get("BrandName", "Unknown"), name=subdrink.get("Name", "Unknown").strip(), 
                            type=style, price=price, link=drink_link, ml=size, percent=percent_alcohol,
                            std_drinks=std_drinks, numb_items=item_numb, efficiency=efficiency, image=image_link,
                            promotion=subdrink.get('IsOnSpecial', False), old_price=subdrink.get("WasPrice", 0))
                result.append(item)
                
        return result, None

class ColesGroupProcessor(RetailerProcessor):
    """
    Base processor for Coles-owned retailers (Liquorland, First Choice).
    Handles both API JSON responses and Next.js __NEXT_DATA__ extraction.
    """
    def __init__(self, store_id: str):
        super().__init__()
        self.store_id = store_id

    def discover_tasks(self, url: str) -> List[dict]:
        """
        Discovery for Coles sites: determines page count and seeds queue.
        """
        content = self.fetch_url(url)
        if not content:
            return [{"url": url, "metadata": {"page": 1}}]

        try:
            # Try to find Next.js data first as it's often more complete
            import re
            next_data_match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', content)
            if next_data_match:
                data = json.loads(next_data_match.group(1))
                # Navigate to the product count in Next.js structure
                # Typically: props.pageProps.initialData.searchResult.pagination.totalResults
                # Or similar. We'll fallback to a simple 1-page task if complex.
                pagination = self._nested_get(data, ["props", "pageProps", "initialData", "searchResult", "pagination"])
                if pagination:
                    total = pagination.get("totalResults", 0)
                    page_size = pagination.get("pageSize", 24)
                    num_pages = (total + page_size - 1) // page_size
                    return [{"url": url + f"?page={p}", "metadata": {"page": p}} for p in range(1, num_pages + 1)]
            
            # Fallback for direct API responses
            data = json.loads(content)
            total = data.get("totalResults", 0)
            if total > 0:
                page_size = 24
                num_pages = (total + page_size - 1) // page_size
                return [{"url": url + f"?page={p}", "metadata": {"page": p}} for p in range(1, num_pages + 1)]
        except:
            pass

        return [{"url": url, "metadata": {"page": 1}}]

    def get_items(self, url: str, metadata: Optional[dict] = None) -> Tuple[List[Item], Optional[dict]]:
        result = []
        content = self.fetch_url(url)
        if not content:
            return result, None

        data = None
        try:
            # Try parsing as direct JSON first (API case)
            data = json.loads(content)
        except:
            # Fallback to Next.js extraction (HTML case)
            import re
            match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', content)
            if match:
                try: data = json.loads(match.group(1))
                except: pass

        if not data:
            return result, None

        # Navigate to products. 
        # API usually has 'results' or 'products' at top level.
        # Next.js has it deep in 'props.pageProps.initialData.searchResult.products'
        products = data.get("results") or data.get("products")
        if not products:
            products = self._nested_get(data, ["props", "pageProps", "initialData", "searchResult", "products"])

        if not products:
            return result, None

        for p in products:
            try:
                name = p.get("productName", "Unknown")
                brand = p.get("brand", "Unknown")
                
                # Pricing
                price_obj = p.get("price", {})
                current_price = self.clean_numeric(price_obj.get("current"))
                was_price = self.clean_numeric(price_obj.get("was"))
                
                # Attributes
                vol = self.parse_volume(name)
                abv = 0.0
                std_drinks = 0.0
                
                # Look in attributes list if available
                for attr in p.get("attributes", []):
                    attr_name = attr.get("name", "").lower()
                    if "alcohol" in attr_name or "abv" in attr_name:
                        abv = self.clean_numeric(attr.get("value"))
                    elif "standard drinks" in attr_name:
                        std_drinks = self.clean_numeric(attr.get("value"))

                # Image and Link
                p_id = p.get("productId")
                slug = p.get("slug")
                link = f"https://www.{self.store_id}.com.au/products/{slug}-{p_id}" if slug else url
                image = p.get("image", {}).get("url") or ""
                if image and not image.startswith("http"):
                    image = f"https://www.{self.store_id}.com.au" + image

                efficiency = (std_drinks / current_price) if current_price > 0 and std_drinks > 0 else 0.0

                item = Item(
                    store=self.store_id,
                    brand=brand,
                    name=name.replace(brand, "").strip(),
                    type="Other", # Will be refined by processors or categorization logic
                    price=current_price,
                    link=link,
                    ml=vol,
                    percent=abv,
                    std_drinks=std_drinks,
                    numb_items=1, # Usually singles in Coles API, multi-packs are separate products
                    efficiency=efficiency,
                    image=image,
                    promotion=was_price > current_price,
                    old_price=was_price
                )
                result.append(item)
            except Exception as e:
                print(f"Error parsing {self.store_id} product: {e}")

        return result, None

    def _nested_get(self, data, keys):
        for key in keys:
            if isinstance(data, dict):
                data = data.get(key)
            else:
                return None
        return data

class LiquorlandProcessor(ColesGroupProcessor):
    def __init__(self):
        super().__init__("liquorland")

class FirstChoiceProcessor(ColesGroupProcessor):
    def __init__(self):
        super().__init__("firstchoiceliquor")
