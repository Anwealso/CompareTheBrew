import json
import math
import re
import asyncio
import os
import ssl
import urllib.request
import urllib.parse
from bs4 import BeautifulSoup
from typing import List, Optional, Tuple
from scripts.classItem import Item
from scraping.processor import RetailerProcessor
from config import Config
from db.databaseHandler import create_connection, get_drink_by_store_link

BRIGHTDATA_ENABLED = False
LIQOURLAND_MAX_RESULTS_PER_PAGE = 80 if BRIGHTDATA_ENABLED else 60
FIRSTCHOICE_MAX_RESULTS_PER_PAGE = 60


class LiquorlandProcessor(RetailerProcessor):
    """
    Liquorland-specific processor implementing the first principles scraping strategy.
    """
    def __init__(self):
        super().__init__()
        self.store_id = "liquorland"

    def _infer_category_from_url(self, url: str) -> str:
        """
        Derive a product category label from the crawl URL so the search_text includes generic terms
        like "wine" / "beer" / "spirits". Falls back to "Other" when nothing matches.
        """
        if not url:
            return "Other"

        parsed = urllib.parse.urlparse(url)
        segments = [segment for segment in parsed.path.split("/") if segment]
        if not segments:
            return "Other"

        primary = segments[0].lower()

        if primary == "spirits" and len(segments) > 1 and segments[1].lower() in {"premixed", "premix"}:
            return "Premixed Spirits"

        if primary in {"wine", "beer", "spirits"}:
            return primary.capitalize()

        return primary.capitalize()

    def _extract_pack_quantity(self, name: str) -> int:
        """
        Parse pack quantity heuristics from the product name.
        """
        text = (name or "").lower()
        patterns = [
            r'pack of\s+(\d+)',
            r'(\d+)\s*(?:pack|pk|packs|pkgs|carton|case|ctn|bottle|bottles|can|cans)\b',
            r'(\d+)\s*[x×]\s*\d+',
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    qty = int(match.group(1))
                    if qty >= 1:
                        return qty
                except (TypeError, ValueError):
                    continue
        return 1

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
        if BRIGHTDATA_ENABLED:
            return self._fetch_with_playwright_click(url)
        else:
            return self.fetch_url(url)

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

                    button_max = page.locator(f'button[aria-label="Click to show {LIQOURLAND_MAX_RESULTS_PER_PAGE} results per page"]')
                    if await button_max.count() > 0:
                        await button_max.click()
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

        result_count_elem = soup.find("div", class_="resultCount")
        if result_count_elem:
            result_text = result_count_elem.get_text(strip=True)
            match = re.search(r'(\d+)\s*products?', result_text)
            if match:
                total_results = int(match.group(1))
                num_pages = math.ceil(total_results / LIQOURLAND_MAX_RESULTS_PER_PAGE)
                return [{"url": url + f"?page={p}", "metadata": {"page": p}} for p in range(1, num_pages + 1)]

        return [{"url": url, "metadata": {"page": 1}}]

    def get_details_from_item_page(self, url: str) -> dict:
        """
        Visit the product page to extract additional details not present in the search page card.
        Returns a dict with percent, std_drinks, and any other additional details.
        """
        details = {
            "percent": 0.0,
            "std_drinks": 0.0,
        }

        if not url:
            return details

        cached = self._get_cached_details(url)
        if cached:
            print(f"[temp_scraper_debug] LiquorlandProcessor cache hit for {url}")  # TODO: Remove this temp_scraper_debug print info.
            return cached

        content = self.fetch_url(url)
        if not content:
            return details

        soup = BeautifulSoup(content, "html.parser")

        props_list = soup.find("ul", class_="product-properties")
        if props_list:
            items = props_list.find_all("li")
            for item in items:
                key_elem = item.find("span", class_="key")
                val_elem = item.find("span", class_="val")
                if not key_elem or not val_elem:
                    continue

                key = key_elem.get_text(strip=True)
                val = val_elem.get_text(strip=True)

                if key == "Standard Drinks":
                    try:
                        details["std_drinks"] = float(val)
                    except ValueError:
                        pass
                elif key == "Alcohol Content":
                    match = re.search(r'([\d.]+)%?', val)
                    if match:
                        try:
                            details["percent"] = float(match.group(1))
                        except ValueError:
                            pass

        return details

    def _get_cached_details(self, url: str, pack_qty: Optional[int] = None) -> Optional[dict]:
        if not url:
            return None
        conn = create_connection()
        if not conn:
            return None
        try:
            resolved_pack_qty = pack_qty if pack_qty is not None else 1
            row = get_drink_by_store_link(conn, self.store_id, url, resolved_pack_qty)
        finally:
            conn.close()

        if not row:
            print(f"[temp_scraper_debug] LiquorlandProcessor cache miss (no row) for {url}")  # TODO: Remove this temp_scraper_debug print info.
            return None

        percent = float(row[9]) if row[9] is not None else 0.0
        std_drinks = float(row[10]) if row[10] is not None else 0.0
        if percent > 0 and std_drinks > 0:
            print(f"[temp_scraper_debug] LiquorlandProcessor cache hit with healthy data for {url}")  # TODO: Remove this temp_scraper_debug print info.
            return {"percent": percent, "std_drinks": std_drinks}
        print(f"[temp_scraper_debug] LiquorlandProcessor cache hit but data invalid for {url} (percent={percent}, std_drinks={std_drinks})")  # TODO: Remove this temp_scraper_debug print info.
        return None

    def get_items(self, url: str, metadata: Optional[dict] = None) -> Tuple[List[Item], Optional[dict]]:
        """
        Extract items from Liquorland using direct HTML extraction.
        For 'page' tasks: extracts items and returns drink_detail URLs for enqueuing.
        For 'drink_detail' tasks: fetches the detail page and updates cached item.
        """        
        result = []
        print(f"[temp_scraper_debug] enter LiquorlandProcessor.get_items(url={url})")  # TODO: Remove this temp_scraper_debug print info.
        content = self.fetch_url_max_rpp(url)
        if not content:
            return result, None

        soup = BeautifulSoup(content, "html.parser")
        product_tiles = soup.find_all("div", class_="ProductTileV3")
        category_type = self._infer_category_from_url(url)

        for tile in product_tiles:
            try:
                product_id = tile.get("data-product-id", "")

                brand_elem = tile.find("div", class_="product-brand")
                brand = brand_elem.get_text(strip=True) if brand_elem else ""

                name_elem = tile.find("div", class_="product-name")
                name = name_elem.get_text(strip=True) if name_elem else ""

                price = 0.0
                current_price_elem = tile.find("span", class_="PriceTagV3")
                if current_price_elem:
                    dollar_elem = current_price_elem.find("span", class_="dollarAmount")
                    cents_elem = current_price_elem.find("span", class_="centsAmount")
                    if dollar_elem:
                        dollars = dollar_elem.get_text(strip=True)
                        cents = cents_elem.get_text(strip=True) if cents_elem else "00"
                        try:
                            price = float(f"{dollars}.{cents}")
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
                pack_qty = self._extract_pack_quantity(name)

                percent = 0.0
                std_drinks = 0.0

                item = Item(
                    store=self.store_id,
                    brand=brand,
                    name=name,
                    type=category_type,
                    price=price,
                    link=link,
                    ml=vol,
                    percent=percent,
                    std_drinks=std_drinks,
                    pack_qty=pack_qty,
                    efficiency=0.0,
                    image=image,
                    promotion=bool(promotion_text),
                    old_price=old_price,
                )
                result.append(item)
            except Exception as e:
                print(f"Error parsing Liquorland HTML product: {e}")

        return result, None

    def build_detail_tasks(self, items: List[Item]) -> List[dict]:
        tasks = []
        for item in items:
            pack_qty = getattr(item, "pack_qty", 1) or 1
            if item.link and not self._get_cached_details(item.link, pack_qty):
                tasks.append({
                    "url": item.link,
                    "metadata": {
                        "store": item.store,
                        "brand": item.brand,
                        "name": item.name,
                        "link": item.link,
                        "pack_qty": pack_qty
                    }
                })
        print(f"[temp_scraper_debug] LiquorlandProcessor.build_detail_tasks created {len(tasks)} tasks")  # TODO: Remove this temp_scraper_debug print info.
        return tasks

    def process_drink_detail(self, url: str, metadata: Optional[dict] = None) -> dict:
        """
        Process a drink detail page task.
        Fetches the detail page and returns the additional details.
        """
        print(f"[temp_scraper_debug] enter LiquorlandProcessor.process_drink_detail(url={url}, metadata={metadata})")  # TODO: Remove this temp_scraper_debug print info.
        details = self.get_details_from_item_page(url)
        print(f"[temp_scraper_debug] LiquorlandProcessor.process_drink_detail returning {details}")  # TODO: Remove this temp_scraper_debug print info.
        return details
