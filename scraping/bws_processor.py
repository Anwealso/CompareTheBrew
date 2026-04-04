import json
import re
from typing import List, Optional, Tuple
from entities.drink_item import DrinkItem
from scraping.processor import RetailerProcessor


class BWSProcessor(RetailerProcessor):
    """
    Processor for BWS (Beer Wine Spirits).
    """

    @staticmethod
    def _extract_pack_qty(subdrink: dict) -> int:
        """
        Extract pack quantity from BWS search JSON.
        Primary source is AdditionalDetails.productunitquantity.
        """
        def parse_qty(raw_value) -> Optional[int]:
            if raw_value is None:
                return None
            if isinstance(raw_value, (int, float)):
                qty = int(float(raw_value))
                return qty if qty > 0 else None

            match = re.search(r"\d+(?:\.\d+)?", str(raw_value))
            if not match:
                return None

            try:
                qty = int(float(match.group(0)))
            except (TypeError, ValueError):
                return None
            return qty if qty > 0 else None

        details = subdrink.get("AdditionalDetails", []) or []
        detail_map = {}
        for detail in details:
            name = detail.get("Name")
            if not name:
                continue
            detail_map[name.lower()] = detail.get("Value")

        qty = parse_qty(detail_map.get("productunitquantity"))
        if qty is not None:
            return qty

        # Fallbacks if productunitquantity is missing
        qty = parse_qty(detail_map.get("displayunitquantity"))
        if qty is not None:
            return qty

        qty = parse_qty(subdrink.get("DisplayQuantity"))
        if qty is not None:
            return qty

        return 1

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
            total_count = data.get("TotalProductCount", 0)
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

    def get_items(
        self, url: str, metadata: Optional[dict] = None
    ) -> Tuple[List[Item], Optional[dict]]:
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

        if "Bundles" not in data:
            return result, None

        bundles = data["Bundles"]
        for drink in bundles:
            products = drink.get("Products", [])
            for subdrink in products:
                # Robust extraction logic with defaults
                parentcode = "None"
                item_num = self._extract_pack_qty(subdrink)
                percent_alcohol = 0.0
                image_num = "None"
                std_drinks = 0.0
                link = "None"
                style = "None"
                size = 0.0

                # Iterate through BWS additional details to populate item properties
                for i in subdrink.get("AdditionalDetails", []):
                    name = i.get("Name")
                    val = i.get("Value")
                    if val is None or val == "":
                        continue

                    if name == "parentstockcode":
                        parentcode = val
                    elif name == "alcohol%":
                        # Strip '%' and convert to float
                        try:
                            percent_alcohol = float(
                                str(val).replace("%", "").strip()
                            )
                        except:
                            percent_alcohol = 0.0
                    elif name == "image1":
                        image_num = val
                    elif name == "standarddrinks":
                        # Handle 'Approx.' prefix and trailing text
                        try:
                            std_val = (
                                str(val)
                                .replace("Approx.", "")
                                .replace("Approx", "")
                                .strip()
                                .split(" ")[0]
                            )
                            std_drinks = float(std_val)
                        except:
                            std_drinks = 0.0
                    elif name == "bwsproducturl":
                        link = val
                    elif name == "standardcategory":
                        style = val
                    elif name == "liquorsize":
                        # Parse size, handling 'Pack of X' and 'L' vs 'ml' units
                        sz = str(val).lower()
                        if "pack" in sz:
                            try:
                                sz = sz.split(" ")[2]
                            except:
                                sz = "0"
                        sz = sz.replace("ml", "").replace("l", "")
                        try:
                            size = float(sz)
                            if (
                                "l" in str(val).lower()
                                and "ml" not in str(val).lower()
                            ):
                                size *= 1000  # Convert Liters to milliliters
                        except:
                            size = 0.0

                drink_link = f"https://bws.com.au/product/{parentcode}/{link}"
                image_link = f"https://edgmedia.bws.com.au/bws/media/products/{image_num}"

                # Safe price conversion (BWS API may expose dollars or cents)
                price_dollars = 0.0
                try:
                    p_val = subdrink.get("Price")
                    if p_val is not None:
                        price_dollars = float(p_val)
                    else:
                        p_val_cents = subdrink.get("price_cents")
                        if p_val_cents is not None:
                            price_dollars = float(p_val_cents) / 100.0
                except (TypeError, ValueError):
                    price_dollars = 0.0

                old_price = 0.0
                try:
                    old_price_val = subdrink.get("WasPrice")
                    if old_price_val is not None:
                        old_price = float(old_price_val)
                    else:
                        old_price_cents_val = subdrink.get("Wasprice_cents")
                        if old_price_cents_val is not None:
                            old_price = float(old_price_cents_val) / 100.0
                except (TypeError, ValueError):
                    old_price = 0.0

                # Map BWS fields to the common Item class
                item_name = subdrink.get("Name", "Unknown").strip()

                if self.progress_callback:
                    self.progress_callback(item_name)

                zero_alc_flag = self.is_zero_alc(percent_alcohol)
                item = DrinkItem(
                    store="bws",
                    brand=subdrink.get("BrandName", "Unknown"),
                    name=item_name,
                    type=style,
                    price=price_dollars,
                    link=drink_link,
                    ml=size,
                    percent=percent_alcohol,
                    std_drinks=std_drinks,
                    pack_qty=item_num,
                    score=None,
                    image=image_link,
                    promotion=subdrink.get("IsOnSpecial", False),
                    old_price=old_price,
                    zero_alc=zero_alc_flag,
                )
                result.append(item)

        return result, None
