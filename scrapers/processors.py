import json
import ssl
from abc import ABC, abstractmethod
from typing import List, Optional
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
        
        Uses ScrapingBee if an API key is provided in the configuration, 
        otherwise falls back to a direct fetch with SSL verification disabled.
        
        Args:
            url (str): The URL to fetch.
            
        Returns:
            Optional[str]: The decoded content of the URL, or None if the fetch fails.
        """
        if self.api_key:
            import urllib.parse
            encoded_url = urllib.parse.quote(url)
            # ScrapingBee API endpoint
            sb_url = f"https://app.scrapingbee.com/api/v1/?api_key={self.api_key}&url={encoded_url}&render_js=true&premium_proxy=true&country_code=au"
            try:
                # Create unverified context for ScrapingBee as well just in case
                context = ssl._create_unverified_context()
                with urlopen(sb_url, context=context) as response:
                    return response.read().decode('utf-8')
            except Exception as e:
                print(f"ScrapingBee error for {url}: {e}")
                # Fallback to direct fetch if ScrapingBee fails
        
        # Direct fetch with unverified SSL context
        try:
            context = ssl._create_unverified_context()
            req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urlopen(req, context=context) as response:
                return response.read().decode('utf-8')
        except Exception as e:
            print(f"Direct fetch error for {url}: {e}")
            return None

    @abstractmethod
    def get_items(self, url: str) -> List[Item]:
        """
        Extracts items from the given URL.
        
        Must be implemented by retailer-specific subclasses.
        
        Args:
            url (str): The URL to scrape.
            
        Returns:
            List[Item]: A list of Item objects extracted from the page.
        """
        pass

class BWSProcessor(RetailerProcessor):
    """
    Processor for BWS (Beer Wine Spirits).
    Handles parsing the BWS JSON API response to extract product details.
    """
    def get_items(self, url: str) -> List[Item]:
        """
        Parses BWS JSON data to extract drinks.
        
        Args:
            url (str): BWS API URL.
            
        Returns:
            List[Item]: List of extracted Item objects.
        """
        result = list()
        
        content = self.fetch_url(url)
        if not content:
            return result
            
        try:
            data = json.loads(content)
        except Exception as e:
            print(f"Error parsing JSON from BWS URL {url}: {e}")
            return result
        
        # get specific json sections
        if 'Bundles' not in data:
            print(f"Key 'Bundles' not found in BWS response from {url}")
            return result
            
        bundles = data['Bundles']
        for drink in bundles:
            products = drink.get('Products', [])
            for subdrink in products:
                # compute from {additionaldetails} section
                parentcode = "None"
                item_numb = 1.0
                percent_alcohol = "None"
                image_numb = "None"
                std_drinks = -1.0
                link = "None"
                style = "None"
                size = "None"
                
                for i in subdrink.get("AdditionalDetails", []):
                    if i["Name"] == "parentstockcode":
                        parentcode = i["Value"] # important for URL
                    elif i["Name"] == "productunitquantity":
                        try:
                            item_numb = float(i["Value"]) # quantity of the item (a la cans in a pack)
                        except:
                            item_numb = 1.0
                    elif i["Name"] == "alcohol%":
                        percent_alcohol = i["Value"] # alcohol percentage
                    elif i["Name"] == "image1":
                        image_numb = i["Value"]
                    elif i["Name"] == "standarddrinks":
                            std_drinks_val = i["Value"]
                            std_drinks_val = std_drinks_val.replace('Approx.', '')
                            std_drinks_val = std_drinks_val.replace('Approx', '')
                            std_drinks_val = std_drinks_val.split(" ")[0]
                            try:
                                std_drinks = float(std_drinks_val.strip())
                            except:
                                std_drinks = -1.0
                    elif i["Name"] == "bwsproducturl":
                        link = i["Value"]
                    elif i["Name"] == "standardcategory":
                        style = i["Value"]
                    elif i["Name"] == "liquorsize":
                        size = i["Value"]
                        if "Pack" in size:
                            try:
                                size = size.split(" ")[2]
                            except:
                                size = "-2"
                        if "ml" in size:
                            size = size.split("ml")[0]
                        elif "mL" in size:
                            size = size.split("mL")[0]
                        elif "L" in size:
                            try:
                                size = float(size.split("L")[0]) * 1000
                            except:
                                size = "-2"
                        try:
                            size = float(size)
                        except:
                            size = "-2"
                        
                drink_link = f"https://bws.com.au/product/{parentcode}/{link}"
                image_link = f"https://edgmedia.bws.com.au/bws/media/products/{image_numb}"
                
                if std_drinks <= 0:
                    # search through json to find proper std drinks
                    for p in products:
                        for subsection in p.get("AdditionalDetails", []):
                            if subsection["Name"] == "standarddrinks":
                                std_drinks_val = subsection["Value"]
                                std_drinks_val = std_drinks_val.replace('Approx.', '')
                                std_drinks_val = std_drinks_val.replace('Approx', '')
                                try:
                                    std_drinks = float(std_drinks_val.strip())
                                except:
                                    std_drinks = -2.0
                                break
                    if std_drinks <= 0:
                        std_drinks = -2.0
                
                if percent_alcohol == "None":
                    for p in products:
                        for subsection in p.get("AdditionalDetails", []):
                            if subsection["Name"] == "alcohol%":
                                percent_alcohol = subsection["Value"]
                
                # Clean up percent_alcohol
                if isinstance(percent_alcohol, str):
                    percent_alcohol = percent_alcohol.replace('%', '').strip()
                    try:
                        percent_alcohol = float(percent_alcohol)
                    except:
                        percent_alcohol = 0.0
                elif percent_alcohol is None:
                    percent_alcohol = 0.0
                                
                if size == "None" or size == "-2":
                    for p in products:
                        for subsection in p.get("AdditionalDetails", []):
                            if subsection["Name"] == "liquorsize":
                                size = subsection["Value"]
                                # Note: simplified size parsing here as in scrape.py
                                if "ml" in size:
                                    size = size.split("ml")[0]
                                elif "mL" in size:
                                    size = size.split("mL")[0]
                                elif "L" in size:
                                    try:
                                        size = str(float(size.split("L")[0]) * 1000)
                                    except:
                                        pass
                
                # Clean up size (ml)
                if isinstance(size, str):
                    try:
                        size = float(size.strip())
                    except:
                        size = 0.0
                elif size is None:
                    size = 0.0
                                    
                if style == "None":
                    for p in products:
                        for subsection in p.get("AdditionalDetails", []):
                            if subsection["Name"] == "standardcategory":
                                style = subsection["Value"]
                    
                
                efficiency = 0.0
                price = 0.0
                try:
                    price_val = subdrink.get("Price")
                    if price_val is not None:
                        price = float(price_val)
                    
                    total_std_drinks = std_drinks
                    if item_numb != 1 and std_drinks != -2 and std_drinks != -1:
                        total_std_drinks = item_numb * std_drinks # account for multiple items in a pack
                    
                    if price > 0 and total_std_drinks > 0:
                        efficiency = total_std_drinks / price
                except:
                    # print("\t", "failed ->", std_drinks, item_numb, subdrink.get("Price"))
                    pass
            
                # store as class
                item = Item(store="bws", brand=subdrink["BrandName"], name=subdrink["Name"].strip(), 
                            type=style, price=price, link=drink_link, ml=size, percent=percent_alcohol,
                            std_drinks=std_drinks, numb_items=item_numb, efficiency=efficiency, image=image_link,
                            promotion=subdrink.get('IsOnSpecial', False), old_price=subdrink.get("WasPrice", 0))
                
                result.append(item)
                
        return result



class LiquorlandProcessor(RetailerProcessor):
    def get_items(self, url: str) -> List[Item]:
        print(f"Liquorland: simulating scrape for {url}")
        # Dummy data generation to demonstrate the pipeline
        items = []
        for i in range(5):
            items.append(Item(
                store="ll", brand="Dummy Brand", name=f"LL Beer {i}", type="Beer",
                price=20.0, link=url, ml=375, percent=4.5, std_drinks=1.3,
                numb_items=6, efficiency=0.39, image="", promotion=False, old_price=20.0
            ))
        return items

class FirstChoiceProcessor(RetailerProcessor):
    def get_items(self, url: str) -> List[Item]:
        print(f"FirstChoice: simulating scrape for {url}")
        # Dummy data generation to demonstrate the pipeline
        items = []
        for i in range(5):
            items.append(Item(
                store="fc", brand="Generic", name=f"FC Wine {i}", type="Wine",
                price=15.0, link=url, ml=750, percent=13.0, std_drinks=7.7,
                numb_items=1, efficiency=0.51, image="", promotion=True, old_price=18.0
            ))
        return items

