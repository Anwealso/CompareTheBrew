from bs4 import BeautifulSoup
import json

with open("first_principles_desktop_stealth_js.html", "r", encoding="utf-8") as f:
    html = f.read()

soup = BeautifulSoup(html, "html.parser")
scripts = soup.find_all("script")

# We know Script 48 is the one (index 48)
s = scripts[48]
content = s.string if s.string else ""

try:
    data = json.loads(content)
    with open("liquorland_wine_data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print("Successfully extracted Schema.org data to liquorland_wine_data.json")
    
    # Summary
    items = data.get("itemListElement", [])
    print(f"Found {len(items)} items in ItemList")
    for item in items[:5]:
        p = item.get("item", {})
        print(f"- {p.get('name')} | {p.get('offers', {}).get('price')}")
except Exception as e:
    print(f"Error: {e}")
