# Scraping Guidelines

Minimal-BS guide to setting up ScrapingBee for JS-heavy, anti-bot-protected sites.

## 1. Account & API Key

Sign up at ScrapingBee and copy your API key. Python or Node are best supported.

## 2. Core Configuration

| Parameter | Why |
|-----------|-----|
| `render_js=true` | Executes JavaScript |
| `premium_proxy=true` | Uses residential IPs |
| `country_code=au` | Matches realistic geo |
| `wait=3000-6000` | Lets JS + trackers fully load |
| `block_resources=false` | Prevents fingerprint flags |

## 3. Python Example

```python
import requests

API_KEY = "YOUR_API_KEY"
url = "https://example.com"

params = {
    "api_key": API_KEY,
    "url": url,
    "render_js": "true",
    "premium_proxy": "true",
    "country_code": "au",
    "wait": 5000,
    "block_resources": "false",
}

response = requests.get(
    "https://app.scrapingbee.com/api/v1/",
    params=params,
    timeout=60
)

html = response.text
```

## 4. SPA / Lazy-Loaded Content

For React/Vue sites, add wait conditions:

```python
params["wait_for"] = "div.product-list"
```

This waits until the selector exists before returning HTML.

## 5. Anti-Bot Hardening

### A. Rotate Request Timing
```python
import random, time
time.sleep(random.uniform(3, 8))
```

### B. Avoid High Parallelism
Start with 1-2 concurrent requests. Increase slowly if needed.

### C. Match Geography
If scraping AU sites, use `country_code="au"`. Mismatched IP + content locale is a common silent block.

## 6. CAPTCHA & Blocking

Detect blocking:
```python
if "captcha" in html.lower():
    # retry later or back off
```

Do not loop aggressively - that's how IP pools get burned.

## 7. Cost Control

- Use `render_js=false` when you confirm the page is static
- Cache results aggressively
- Avoid re-scraping unchanged pages
- Prefer HTML parsing over screenshot APIs

Typical hobby usage: 1-5k pages/month is very affordable.

## 8. Escalation Ladder

When ScrapingBee starts failing:

1. Increase wait
2. Enable premium proxies
3. Lower concurrency
4. Add selector-based waits

If a site blocks ScrapingBee entirely, it is usually:
- Actively fingerprinting headless browsers
- Or requires logged-in sessions

At that point, move to Playwright + residential proxies.

## 9. Common Mistakes

- Blocking images/fonts
- Zero wait time
- High concurrency
- Datacenter proxies only
- Identical scrape timing

These scream "bot" even with JS rendering.

## 10. Recommended Default Template

```python
params = {
    "render_js": "true",
    "premium_proxy": "true",
    "country_code": "target_country",
    "wait": 5000,
    "block_resources": "false",
}
# concurrency: 1-2
```

This covers 90% of real-world indie scraping use cases.
