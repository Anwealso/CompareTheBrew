import asyncio
import ssl
from abc import ABC, abstractmethod
from typing import Optional
from urllib.request import Request, urlopen
from config import Config


class FetcherImpl(ABC):
    @abstractmethod
    def fetch_url(self, url: str) -> Optional[str]:
        pass


class ScrapingBeeFetcherImpl(FetcherImpl):
    def __init__(self):
        self.api_key = Config.SCRAPINGBEE_API_KEY

    def fetch_url(self, url: str) -> Optional[str]:
        if not self.api_key:
            return None
        
        import urllib.parse
        encoded_url = urllib.parse.quote(url)
        sb_url = f"https://app.scrapingbee.com/api/v1/?api_key={self.api_key}&url={encoded_url}&render_js=true&premium_proxy=true&country_code=au"
        try:
            context = ssl._create_unverified_context()
            with urlopen(sb_url, context=context) as response:
                return response.read().decode('utf-8')
        except Exception as e:
            print(f"ScrapingBee error for {url}: {e}")
            return None


class BrightDataFetcherImpl(FetcherImpl):
    def __init__(self, country: str = "au", zip_code: Optional[str] = None):
        self.customer_id = getattr(Config, 'BRIGHTDATA_CUSTOMER_ID', None)
        self.zone = getattr(Config, 'BRIGHTDATA_ZONE', None)
        self.password = getattr(Config, 'BRIGHTDATA_PASSWORD', None)
        self.country = country
        self.zip_code = zip_code

    def _get_ws_endpoint(self) -> Optional[str]:
        if not all([self.customer_id, self.zone, self.password]):
            return None
        
        auth = f"brd-customer-{self.customer_id}-zone-{self.zone}"
        if self.country:
            auth += f"-country-{self.country}"
        if self.zip_code:
            auth += f"-zip-{self.zip_code}"
        auth += f":{self.password}"
        
        return f"wss://{auth}@brd.superproxy.io:9222"

    def fetch_url(self, url: str) -> Optional[str]:
        ws_endpoint = self._get_ws_endpoint()
        if not ws_endpoint:
            print("BrightData: Missing credentials (CUSTOMER_ID, ZONE, or PASSWORD)")
            return None

        try:
            from playwright.async_api import async_playwright
        except ImportError:
            print("BrightData: playwright not installed. Run: pip install playwright")
            return None

        async def _fetch():
            try:
                async with async_playwright() as pw:
                    browser = await pw.chromium.connect_over_cdp(ws_endpoint)
                    page = await browser.new_page()
                    await page.goto(url, timeout=120000)
                    content = await page.content()
                    await browser.close()
                    return content
            except Exception as e:
                print(f"BrightData error for {url}: {e}")
                return None

        return asyncio.run(_fetch())


class Fetcher:
    _instance = None
    _implementation: FetcherImpl = ScrapingBeeFetcherImpl() # use scrapingbee implementation

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def fetch_url(self, url: str) -> Optional[str]:
        if self._implementation is None:
            return self._direct_fetch(url)
        return self._implementation.fetch_url(url)

    def _direct_fetch(self, url: str) -> Optional[str]:
        try:
            context = ssl._create_unverified_context()
            req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urlopen(req, context=context) as response:
                return response.read().decode('utf-8')
        except Exception as e:
            print(f"Direct fetch error for {url}: {e}")
            return None


_fetcher = Fetcher()


def get_fetcher() -> Fetcher:
    return _fetcher
