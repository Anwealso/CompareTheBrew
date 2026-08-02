#!/usr/bin/env python3
"""
Capture real retailer responses into tests/scraping/fixtures/ so the offline
tiers (Tier 1 parsing, Tier 3 pipeline) run against real-world data.

    python tests/scraping/refresh_fixtures.py

WARNING: this spends ScrapingBee credits. It makes ~3 live requests:
  - 1 BWS listing (JSON API)
  - 1 Liquorland listing (HTML, stealth + JS render -> pricier)
  - 1 Liquorland product detail (HTML, derived from the listing)
All requests go through the project's own fetchers, which route via
ScrapingBee (BRIGHTDATA_ENABLED is False) -- never a direct hit on the
retailer, so there is no ban risk. Run it deliberately, not in a loop.

It prints a sanity parse of each capture (item counts, a sample) so you can
eyeball that the retailer hasn't changed their layout before committing the
new fixtures.
"""
import json
import sys
from pathlib import Path

# Make the repo root importable when run as a script.
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from scraping.bws_processor import BWSProcessor  # noqa: E402
from scraping.liquorland_processor import LiquorlandProcessor  # noqa: E402

FIXTURES_DIR = Path(__file__).parent / "fixtures"
GOLDEN_URLS = Path(__file__).parent / "golden_urls.json"


def _load_golden():
    with open(GOLDEN_URLS) as f:
        return json.load(f)


def _write(name: str, content: str):
    path = FIXTURES_DIR / name
    path.write_text(content, encoding="utf-8")
    print(f"  wrote {path.relative_to(REPO_ROOT)} ({len(content):,} bytes)")


def refresh_bws(url: str):
    print("\n=== BWS listing ===")
    proc = BWSProcessor()
    content = proc.fetch_url(url)
    if not content:
        print("  FAILED: no content returned (check SCRAPING_API_KEY / credits)")
        return False
    _write("bws_beer_page1.json", content)
    items, _ = proc.get_items(url_content_override(proc, url, content))
    print(f"  parsed {len(items)} items")
    if items:
        s = items[0]
        print(f"  sample: {s.name!r} ${s.price} {s.percent}% std={s.stdDrinks}")
    return len(items) > 0


def url_content_override(proc, url, content):
    """get_items re-fetches internally; monkeypatch fetch to reuse captured
    content so the sanity parse costs no extra credits."""
    proc.fetch_url = lambda _u: content  # type: ignore
    if hasattr(proc, "fetch_url_max_rpp"):
        proc.fetch_url_max_rpp = lambda _u: content  # type: ignore
    return url


def refresh_liquorland(listing_url: str, detail_fallback: str):
    print("\n=== Liquorland listing ===")
    proc = LiquorlandProcessor()
    content = proc.fetch_url_max_rpp(listing_url)
    if not content:
        print("  FAILED: no content returned (check SCRAPING_API_KEY / credits)")
        return False
    _write("liquorland_beer_page1.html", content)

    # Parse with a fresh processor pointed at the captured content.
    parse_proc = LiquorlandProcessor()
    url_content_override(parse_proc, listing_url, content)
    items, _ = parse_proc.get_items(listing_url)
    print(f"  parsed {len(items)} items")
    detail_url = detail_fallback
    if items:
        s = items[0]
        print(f"  sample: {s.name!r} ${s.price} std={s.stdDrinks} link={s.link}")
        if s.link:
            detail_url = s.link

    print("\n=== Liquorland detail ===")
    print(f"  detail url: {detail_url}")
    detail_proc = LiquorlandProcessor()
    detail_content = detail_proc.fetch_url(detail_url)
    if not detail_content:
        print("  FAILED: no detail content returned")
        return False
    _write("liquorland_product_detail.html", detail_content)
    parse_detail = LiquorlandProcessor()
    parse_detail.fetch_url = lambda _u: detail_content  # type: ignore
    details = parse_detail.get_details_from_item_page(detail_url)
    print(f"  parsed details: {details}")
    return details is not None


def main():
    FIXTURES_DIR.mkdir(exist_ok=True)
    golden = _load_golden()
    # Optional retailer filter: `refresh_fixtures.py bws` or `... ll` captures
    # only that retailer (saves credits when one fixture is already fresh).
    which = sys.argv[1].lower() if len(sys.argv) > 1 else "all"
    print("Refreshing fixtures via ScrapingBee (this spends credits)...")
    ok_bws = ok_ll = True
    if which in ("all", "bws"):
        ok_bws = refresh_bws(golden["bws"]["listing"])
    if which in ("all", "ll", "liquorland"):
        ok_ll = refresh_liquorland(
            golden["ll"]["listing"], golden["ll"].get("detail_fallback", "")
        )
    print("\n=== Summary ===")
    print(f"  BWS:        {'OK' if ok_bws else 'skipped/failed'}")
    print(f"  Liquorland: {'OK' if ok_ll else 'skipped/failed'}")
    if not (ok_bws and ok_ll):
        sys.exit(1)


if __name__ == "__main__":
    main()
