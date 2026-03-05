import os
import ssl
import urllib.parse
import urllib.request
import time
import json
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("SCRAPING_API_KEY")

def try_fetch(name, url, params, headers=None):
    print(f"\n>> Attempting: {name}")
    qs = urllib.parse.urlencode(params)
    sb_url = f"https://app.scrapingbee.com/api/v1/?{qs}"
    
    req = urllib.request.Request(sb_url, headers=headers or {})
    context = ssl._create_unverified_context()
    
    start = time.time()
    try:
        with urllib.request.urlopen(req, context=context, timeout=180) as resp:
            html = resp.read().decode('utf-8')
            filename = f"first_principles_{name}.html"
            with open(filename, "w", encoding="utf-8") as f:
                f.write(html)
            
            status = resp.getcode()
            print(f"   Status: {status} | size: {len(html)} | time: {time.time()-start:.1f}s")
            
            low = html.lower()
            if "captcha" in low or "shieldsquare" in low:
                print("   !! BLOCKED (Captcha/ShieldSquare) !!")
            elif "__next_data__" in low:
                print("   !! SUCCESS (Found __NEXT_DATA__) !!")
            elif len(html) > 50000:
                print("   !! PROBABLE SUCCESS (Large HTML) !!")
            else:
                print("   ?? UNCERTAIN (Small HTML) ??")
                if len(html) < 2000:
                    print(f"   Content snippet: {html[:200]}")
    except Exception as e:
        print(f"   FAILED: {e}")

if __name__ == "__main__":
    wine_url = "https://www.liquorland.com.au/wine"
    
    # 1. Mobile Safari + Premium Proxy + NO JS
    # (Bypasses many JS fingerprints)
    try_fetch("mobile_nojs", wine_url, {
        "api_key": API_KEY,
        "url": wine_url,
        "render_js": "false",
        "premium_proxy": "true",
        "country_code": "au"
    }, {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1"
    })
    
    # 2. Desktop Chrome + Stealth Proxy + JS + Wait for specific element
    # (Heavyweight attempt)
    try_fetch("desktop_stealth_js", wine_url, {
        "api_key": API_KEY,
        "url": wine_url,
        "render_js": "true",
        "premium_proxy": "true",
        "stealth_proxy": "true",
        "country_code": "au",
        "wait": "10000",
        "wait_for": ".product-tile-list"
    }, {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    })
    
    # 3. Googlebot Simulation (Sites sometimes let Googlebot in without ShieldSquare)
    try_fetch("googlebot", wine_url, {
        "api_key": API_KEY,
        "url": wine_url,
        "render_js": "false",
        "premium_proxy": "true",
        "country_code": "au"
    }, {
        "User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    })
    
    # 4. API call with custom headers ( mimicking the site's own frontend calls)
    api_url = "https://www.liquorland.com.au/api/v1/search/wine"
    try_fetch("api_mimic", api_url, {
        "api_key": API_KEY,
        "url": api_url,
        "render_js": "false",
        "premium_proxy": "true",
        "country_code": "au"
    }, {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://www.liquorland.com.au/wine"
    })
