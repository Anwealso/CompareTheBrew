import json
from typing import List, Optional, Tuple
from scripts.classItem import Item
from scraping.processor import RetailerProcessor

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
