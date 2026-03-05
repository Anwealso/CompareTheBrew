from bs4 import BeautifulSoup
import json
import re

with open("first_principles_desktop_stealth_js.html", "r", encoding="utf-8") as f:
    html = f.read()

soup = BeautifulSoup(html, "html.parser")
scripts = soup.find_all("script")

print(f"Found {len(scripts)} script tags")

for i, s in enumerate(scripts):
    content = s.string if s.string else ""
    if len(content) > 1000:
        print(f"Script {i} size: {len(content)}")
        # Check if it looks like JSON
        if content.strip().startswith("{") or content.strip().startswith("["):
            print(f"  -> Looks like JSON! First 100 chars: {content.strip()[:100]}")
        
        # Search for product-like keys
        if "product" in content.lower() or "price" in content.lower():
            print(f"  -> Contains 'product' or 'price'")
            if "Penfolds" in content:
                print(f"  -> Contains 'Penfolds'!")

# Also look for any JSON in the whole file
matches = re.findall(r'\{"productName".*?\}', html)
if matches:
    print(f"Found {len(matches)} productName JSON objects")
    print(f"First match: {matches[0][:200]}")
else:
    print("No productName JSON objects found via regex")
