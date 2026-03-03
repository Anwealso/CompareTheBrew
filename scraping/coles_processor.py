import json
from typing import List, Optional, Tuple
from scripts.classItem import Item
from scraping.processors import RetailerProcessor

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
