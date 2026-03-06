import json
import Math
import re
import asyncio
from bs4 import BeautifulSoup
from typing import List, Optional, Tuple
from scripts.classItem import Item
from scraping.processor import RetailerProcessor
from config import Config

LIQOURLAND_MAX_RESULTS_PER_PAGE = 80

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
    """
    Liquorland-specific processor implementing the first principles scraping strategy.
    Uses multiple fallback strategies to bypass bot protection:
    1. Mobile Safari + Premium Proxy + NO JS (bypasses JS fingerprints)
    2. Desktop Chrome + Stealth Proxy + JS + Wait (heavyweight attempt)
    3. Googlebot Simulation (sometimes bypasses ShieldSquare)
    4. API mimic call (mimics site's own frontend)
    """
    def __init__(self):
        super().__init__("liquorland")

    def fetch_url(self, url: str) -> Optional[str]:
        """
        Override fetch_url with multi-strategy approach for Liquorland.
        Strategy: Desktop Chrome + Stealth Proxy + JS + Wait for element (heavyweight attempt).
        """
        return self._fetch(url, render_js=True, premium_proxy=True, stealth_proxy=True,
                          wait="10000", wait_for=".product-tile-list", custom_headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        })

    def fetch_url_max_rpp(self, url: str) -> Optional[str]:
        """
        Fetches the Liquorland page with maximum results per page (80).
        Uses Playwright to click the button as URL params don't work (site requires JS).
        """
        return self._fetch_with_playwright_click(url)

    def _fetch_with_playwright_click(self, url: str) -> Optional[str]:
        """
        Use Playwright to click the 80 results per page button and return the content.
        """
        customer_id = getattr(Config, 'BRIGHTDATA_CUSTOMER_ID', None)
        zone = getattr(Config, 'BRIGHTDATA_ZONE', None)
        password = getattr(Config, 'BRIGHTDATA_PASSWORD', None)
        
        if not all([customer_id, zone, password]):
            return self.fetch_url(url)
        
        auth = f"brd-customer-{customer_id}-zone-{zone}:{password}"
        ws_endpoint = f"wss://{auth}@brd.superproxy.io:9222"
        
        async def _fetch():
            try:
                from playwright.async_api import async_playwright
            except ImportError:
                print("Playwright not installed")
                return None
            
            try:
                async with async_playwright() as pw:
                    browser = await pw.chromium.connect_over_cdp(ws_endpoint)
                    page = await browser.new_page()
                    
                    await page.goto(url, timeout=120000)
                    
                    button_80 = page.locator('button[aria-label="Click to show 80 results per page"]')
                    if await button_80.count() > 0:
                        await button_80.click()
                        await page.wait_for_load_state("networkidle", timeout=30000)
                    
                    content = await page.content()
                    await browser.close()
                    return content
            except Exception as e:
                print(f"Playwright error for {url}: {e}")
                return None
        
        return asyncio.run(_fetch())

    def _build_sb_url(self, url: str, params: dict, headers: Optional[dict] = None) -> str:
        """Build ScrapingBee URL with given parameters."""
        import urllib.parse
        qs = urllib.parse.urlencode(params)
        sb_url = f"https://app.scrapingbee.com/api/v1/?{qs}"
        return sb_url

    def _fetch(self, url: str, render_js: bool = True, premium_proxy: bool = True,
               stealth_proxy: bool = False, wait: str = "", wait_for: str = "",
               custom_headers: Optional[dict] = None) -> Optional[str]:
        """Execute a ScrapingBee request with given parameters."""
        if not self.api_key:
            print("No API key configured")
            return None
            
        params = {
            "api_key": self.api_key,
            "url": url,
            "render_js": "true" if render_js else "false",
            "country_code": "au",
            "premium_proxy": "true",
            "stealth_proxy": "true",
            "wait": wait,
            "wait_for": wait_for
        }
        
        headers = custom_headers or {}
        
        import ssl
        import urllib.request
        import urllib.parse
        
        encoded_url = urllib.parse.quote(url)
        sb_url = f"https://app.scrapingbee.com/api/v1/?api_key={self.api_key}&url={encoded_url}"
        sb_url += "&render_js=true"
        sb_url += "&premium_proxy=true"
        sb_url += "&stealth_proxy=true"
        sb_url += f"&wait={wait}"
        sb_url += f"&wait_for={urllib.parse.quote(wait_for)}"
        sb_url += "&country_code=au"
        
        try:
            context = ssl._create_unverified_context()
            req = urllib.request.Request(sb_url, headers=headers)
            with urllib.request.urlopen(req, context=context, timeout=180) as resp:
                return resp.read().decode('utf-8')
        except Exception as e:
            return None

    def discover_tasks(self, url: str) -> List[dict]:
        """
        Discovery for Liquorland using Playwright to click 80 items per page
        and extract the page count from the pagination.
        """        
        content = self.fetch_url_max_rpp(url)
        if not content:
            return [{"url": url, "metadata": {"page": 1}}]
        soup = BeautifulSoup(content, "html.parser")

        pager = soup.find("div", class_="PagerQuerystring")
        if pager:
            pager_text = pager.get_text(strip=True)
            match = re.search(r'of\s+(\d+)', pager_text)
            if match:
                num_pages = int(match.group(1))
                return [{"url": url + f"?page={p}", "metadata": {"page": p}} for p in range(1, num_pages + 1)]
        
        result_count_elem = soup.find("div", class_="resultCount")
        if result_count_elem:
            result_text = result_count_elem.get_text(strip=True)
            match = re.search(r'(\d+)\s*products?', result_text)
            if match:
                total = int(match.group(1))
                num_pages = (total + 79) // 80
                return [{"url": url + f"?page={p}", "metadata": {"page": p}} for p in range(1, num_pages + 1)]

        return [{"url": url, "metadata": {"page": 1}}]

    def get_items(self, url: str, metadata: Optional[dict] = None) -> Tuple[List[Item], Optional[dict]]:
        """
        Extract items from Liquorland using direct HTML extraction.
        """        
        result = []
        content = self.fetch_url_max_rpp(url)
        if not content:
            return result, None

        soup = BeautifulSoup(content, "html.parser")
        product_tiles = soup.find_all("div", class_="ProductTileV3")

        for tile in product_tiles:
            try:
                product_id = tile.get("data-product-id", "")
                
                brand_elem = tile.find("div", class_="product-brand")
                brand = brand_elem.get_text(strip=True) if brand_elem else ""
                
                name_elem = tile.find("div", class_="product-name")
                name = name_elem.get_text(strip=True) if name_elem else ""
                
                current_price = 0.0
                current_price_elem = tile.find("span", class_="PriceTagV3")
                if current_price_elem:
                    dollar_elem = current_price_elem.find("span", class_="dollarAmount")
                    cents_elem = current_price_elem.find("span", class_="centsAmount")
                    if dollar_elem:
                        dollars = dollar_elem.get_text(strip=True)
                        cents = cents_elem.get_text(strip=True) if cents_elem else "00"
                        try:
                            current_price = float(f"{dollars}.{cents}")
                        except:
                            pass
                
                old_price = 0.0
                promotion_text = ""
                promotion_elem = tile.find("div", class_="x-for-y")
                if promotion_elem:
                    promo_link = promotion_elem.find("a")
                    if promo_link:
                        promotion_text = promo_link.get_text(strip=True)
                
                thumbnail = tile.find("a", class_="thumbnail")
                link = ""
                if thumbnail:
                    link = thumbnail.get("href", "")
                    if link and not link.startswith("http"):
                        link = f"https://www.liquorland.com.au{link}"
                
                image = ""
                if thumbnail:
                    img_elem = thumbnail.find("img")
                    if img_elem:
                        image = img_elem.get("src", "")
                        if image and not image.startswith("http"):
                            image = f"https://www.liquorland.com.au{image}"
                
                vol = self.parse_volume(name)
                abv = 0.0
                std_drinks = 0.0
                efficiency = (std_drinks / current_price) if current_price > 0 and std_drinks > 0 else 0.0
                
                item = Item(
                    store=self.store_id,
                    brand=brand,
                    name=name,
                    type="Other",
                    price=current_price,
                    link=link,
                    ml=vol,
                    percent=abv,
                    std_drinks=std_drinks,
                    numb_items=1,
                    efficiency=efficiency,
                    image=image,
                    promotion=bool(promotion_text),
                    old_price=old_price
                )
                result.append(item)
            except Exception as e:
                print(f"Error parsing Liquorland HTML product: {e}")

        return result, None

class FirstChoiceProcessor(ColesGroupProcessor):
    def __init__(self):
        super().__init__("firstchoiceliquor")
