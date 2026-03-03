# Liquorland & First Choice Scraping Strategy

Both Liquorland (LL) and First Choice (FC) are owned by Coles Group and share a similar technical stack (Next.js, ShieldSquare bot protection).

## 1. Architecture
We will use a common `ColesGroupProcessor` base class to handle shared logic for these two retailers, as their API responses and HTML structures are almost identical.

## 2. Bot Mitigation
The primary challenge is hCaptcha/ShieldSquare.
- **ScrapingBee Configuration:**
    - `render_js=true` (Essential for Next.js hydration)
    - `premium_proxy=true` (Required to bypass ShieldSquare)
    - `country_code=au` (Must match the site's locale)
    - `wait=5000` (Allows time for the anti-bot script to execute and the page to load)

## 3. Data Extraction Points
We have two main targets for data:
1.  **Internal Search API:** `https://www.liquorland.com.au/api/v1/search/[category]`
    - Returns clean JSON.
    - Fields: `results` array containing `productName`, `brand`, `price`, `image`, etc.
2.  **Next.js Hydration Script:** `#__NEXT_DATA__`
    - Found in the HTML of category pages (e.g., `/beer`).
    - Contains the same JSON structure as the API but is often easier to fetch as it's part of the initial page load.

## 4. Item Mapping
The mapping to our `Item` class will follow this logic:
- **Brand:** `brand` field or extracted from start of `productName`.
- **Name:** `productName` stripped of brand.
- **Price:** `price.current` (Sale price) vs `price.was` (Original price).
- **Volume (ml):** Parsed from name using regex (e.g. `(\d+)\s*(ml|l|ML|L)`).
- **ABV (%):** Often found in `attributes` or parsed from description.
- **Standard Drinks:** Found in `attributes` or calculated.

## 5. Implementation Steps
1.  Refactor `RetailerProcessor` with common parsing utilities.
2.  Implement `ColesGroupProcessor` with robust JSON/HTML parsing.
3.  Inherit `LiquorlandProcessor` and `FirstChoiceProcessor` from the base class.
4.  Update `sitemaps.json` with correct category URLs.
