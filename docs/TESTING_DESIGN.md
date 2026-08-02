# Scraping Test Suite — Design & Methodology

Status: **implemented**. This document is both the design rationale and the
reference for how the suite works. See "As-built summary" for the quick
version; the tier sections below explain the *why*.

## As-built summary

The suite lives in `tests/scraping/` and runs on **real** retailer data
captured once via ScrapingBee and checked in as fixtures. `pytest` runs the
offline tiers with no network and no credits; live tests are opt-in.

```
tests/scraping/
  conftest.py              # pytest fixtures: temp_db, fake_fetchers
  _support.py              # importable helpers: FakeProcessor, load_fixture, default_resolver
  golden_urls.json         # the small fixed URL set (capture + live tests)
  refresh_fixtures.py      # recapture fixtures via ScrapingBee (manual)
  fixtures/
    bws_beer_page1.json          # real BWS Browse API response
    liquorland_beer_page1.html   # real Liquorland listing page
    liquorland_product_detail.html
  test_processor_helpers.py    # Tier 1: clean_numeric/parse_volume/is_zero_alc/calculate_score
  test_bws_processor.py        # Tier 1: BWS get_items/discover_tasks on fixture
  test_liquorland_processor.py # Tier 1: LL get_items/detail/build_detail_tasks on fixture
  test_task_queue.py           # offline: claim/retry/reset + concurrent claim race
  test_controller.py           # offline: discover/run_next with FakeProcessor
  test_pipeline.py             # Tier 3: real pipeline on fake fetchers (task counts + interim drinks state)
  test_live_smoke.py           # Tier 2: @pytest.mark.live fetch+parse + chained locators
```

Run it:

```bash
source venv/bin/activate
pytest                          # 57 offline tests, no network / no credits
pytest -m live                  # 3 live tests via ScrapingBee (spends credits)
python tests/scraping/refresh_fixtures.py       # recapture all fixtures
python tests/scraping/refresh_fixtures.py ll    # recapture just Liquorland
```

Current counts: **57 offline tests + 3 opt-in live tests.** The `live`
marker is excluded by default via `pyproject.toml` `addopts = "-m 'not
live'"`, so plain `pytest` never spends a credit.

**Bugs surfaced by building this suite:**
- *Fixed* — `ScrapingController.run_next()` read claimed `scrape_tasks` rows
  with stale positional indices (from before the `task_type` column was
  added), so `json.loads()` ran on the task_type string and raised on every
  task — the pipeline could not process anything. Indices corrected to
  schema order. Tier 3 now guards this.
- *Open* — `BWSProcessor.discover_tasks` reads `TotalProductCount`, but the
  real API returns `TotalRecordCount`, so it always falls back to a single
  page and under-paginates any category with ≥1000 products. Pinned by a
  test (`test_bws_processor.py`) documenting current behavior; not yet
  fixed because the fix increases per-run ScrapingBee spend.

## Goals

Three distinct jobs this suite does, and which tier serves each:

1. **"Have I broken processing as I develop?"** — a regression net. Fast,
   offline, deterministic, run constantly. Covers processor parsing logic
   and the full queue→processor→DB pipeline (task counts, interim drinks
   state) against frozen real-world fixtures, with zero network or credits.
   → **Tier 1** (parsing) + **Tier 3** (pipeline) + offline queue tests.

2. **"Does my code still work on the live site — have they changed their
   layout?"** — a drift detector. Hits the real retailers against a tiny
   fixed URL set, loose assertions, run on demand (before a scraping PR,
   or when something looks off). This is also what tells you a fixture has
   gone stale and needs refreshing. → **Tier 2** (`pytest -m live`).

3. **"Help me build a processor for a NEW site without burning credits."**
   — a development harness. Capture the new site's page(s) *once* via a
   real fetch, save as a fixture, then iterate on the new processor
   entirely offline (Tier 1 replay) until parsing is right — spending one
   scrape instead of hundreds. The `refresh_fixtures.py` capture script
   and the fake-fetcher pipeline plumbing are the same machinery that make
   this cheap; a new site just needs a fixture file, a golden URL entry,
   and a processor class. → **Tier 1 + Tier 3 workflow**, see "New-site
   processor workflow" below.

Underlying all three: stay cheap and fast enough to run locally on demand.
No CI wiring for now — this is a `pytest` you run yourself.

## Non-goals (for this pass)

- No CI integration, no scheduled drift detection. Can be layered on later
  once the suite exists and is trusted.
- Not trying to mock ScrapingBee — per your call, the suite hits real
  retailer endpoints for the processor tests, just against a small, fixed
  set of URLs, so cost stays bounded rather than zero.

## The organizing principle

The system has exactly **one boundary with external unpredictability**: the
fetch + parse step, where real retailer HTML/JSON enters our code. Retailers
can redesign a page overnight; nothing else in the system does. Everything
inside that boundary — the task queue, the controller's orchestration, the
DB writes and dedup — is deterministic internal logic that behaves the same
every run.

The tiers split on that boundary:
- **Only the fetch + parse seam needs live testing** (Tier 2), because it's
  the only part that can silently break due to something outside our repo.
- **Everything inside is tested deterministically and offline** (Tier 1
  parsing replay, Tier 3 pipeline/orchestration, offline queue tests) —
  free, fast, exact assertions, run constantly.

## Test tiers

### Tier 1 — pure unit tests (no network, always run, milliseconds)

Everything in `scraping/processor.py` that's a pure function operating on
already-fetched HTML/JSON: `clean_numeric`, `parse_volume`, `is_zero_alc`,
`calculate_score` (in `databaseHandler.py`), and the parsing logic inside
`BWSProcessor.get_items` / `LiquorlandProcessor.get_items` *given a fixed
input string*.

These need input data too — but instead of hand-writing fake HTML/JSON
(which drifts from reality and misses edge cases), we capture it from the
same small set of real URLs used in Tier 2, and check it into the repo as
a fixture once fetched. Practically: Tier 2's live fetch is the *source*
of the fixture; Tier 1 replays the saved copy so parsing logic gets
exercised on every `pytest` run for free, and only the network fetch
itself is gated behind the `live` marker.

```
tests/scraping/fixtures/
  bws_beer_page1.json          # saved BWS API response
  liquorland_beer_page1.html   # saved Liquorland listing page
  liquorland_product_detail.html
```

The checked-in fixtures are **real** responses captured once via ScrapingBee
(BWS 31 items, LL listing 60 items, LL detail parsed to `percent=3.5,
std_drinks=1.0`). `tests/scraping/refresh_fixtures.py` (run manually)
re-fetches them from the golden URL list through the project's own fetchers
— which route via ScrapingBee, never a direct retailer hit — and overwrites
the files. That is the one place actual scraping happens outside Tier 2.

### Tier 2 — live fetch + parse (real network, opt-in, `pytest -m live`)

**Scope: fetch a real page and run it through the processor's parse logic —
that's it.** No controller, no task queue, no DB. Tier 2 exercises *only*
the one seam that touches the unpredictable outside world; orchestration
and persistence are deterministic internal logic and are fully covered by
Tier 3 offline. Concretely each live test is roughly:
`html = real_fetch(golden_url)` → `items = processor.get_items(html/url)` →
assert the items look sane. (For Liquorland's detail seam, same shape with
`process_drink_detail` against a real product URL.)

Marked `@pytest.mark.live`, excluded from the default `pytest` run via
`pyproject.toml`'s `addopts = "-m 'not live'"` (or a `conftest.py`
`collection_modifyitems` skip) so plain `pytest` never spends a credit by
accident. Run explicitly with `pytest -m live` when you want to check
whether a retailer changed something.

**Validating extracted locators (chained fetch + parse).** Some parse
outputs aren't data — they're *instructions for the next fetch*: the
next-page URL in `get_items`'s `next_metadata`, the per-item detail URLs
from `build_detail_tasks`, and the seed/page URLs from `discover_tasks`. A
link is only "good" if it resolves to a real page the next parse stage can
actually consume — which you genuinely can't verify without fetching it.
But that still does **not** require the queue/controller/DB. You validate
it by chaining fetch → parse → fetch → parse *by hand*, following each
extracted locator exactly once:

```python
items, next_meta = processor.get_items(real_listing_url)   # fetch #1 + parse
detail_url = processor.build_detail_tasks(items)[0]["url"]  # locator the parser produced
next_url   = next_meta["next_url"]

d = processor.process_drink_detail(detail_url)              # fetch #2 → proves detail link is live-good
assert d and d["percent"] is not None

items2, _ = processor.get_items(next_url)                   # fetch #3 → proves pagination link is live-good
assert len(items2) > 0
```

Why this stays in Tier 2 and the queue/controller/DB stays out:

- The unpredictable thing under test is still only "does this live URL
  resolve and parse" — the same fetch+parse seam, followed one hop forward.
  The logic that *extracts* the URL lives in the processor
  (`build_detail_tasks`, `next_metadata`, `discover_tasks`), not the
  controller.
- Whether an extracted URL is then correctly carried through
  enqueue → claim → persist is deterministic internal logic, already proven
  in Tier 3 against fixtures. Running it live adds no new signal about
  orchestration — only about the link, which the manual chain already gives.
- Practically it's the difference between ~3 fetches and a credit blowout:
  following each locator *once by hand* is a handful of calls, whereas
  handing the same URLs to the real controller fans out to a `drink_detail`
  task per item and drains the whole queue — dozens of live fetches, the
  exact thing we're avoiding.

So the boundary holds: "chained fetch+parse" is still the fetch+parse seam;
it just proves the parser's *output links* are as valid as its *output
data*. (BWS is analogous: its page URLs come from `discover_tasks` rather
than `next_metadata`, so the live test calls `discover_tasks` and fetches
one generated page URL to confirm it parses.)

**Golden URL set** — small and fixed, reviewed by you, one JSON file:

```
tests/scraping/golden_urls.json
{
  "bws": {"listing": "<real beer listing API URL, small pageSize>"},
  "ll":  {"listing": "<real beer listing URL>",
          "detail_fallback": "<real product detail URL>"}
}
```

(The LL detail URL used at capture time is derived from the freshly-fetched
listing's first item; `detail_fallback` is only used if that derivation
fails.)

Target for a full `pytest -m live` run, following the chained-locator
pattern below:
- **Liquorland**: listing + its next-page + one detail = 3 fetches.
- **BWS**: `discover_tasks` count-probe + one generated page = ~2 fetches.

≈ 5–6 live HTTP calls total. Cheap enough to run before every scraping PR,
and each one is deliberate (one hop per locator), never a full drained run.

What Tier 2 actually asserts: the fetch succeeds (no CAPTCHA, non-empty
response), and running it through the real processor produces at least N
`DrinkItem`s with sane values (price > 0, link non-empty, etc.) — it's a
smoke test for "did the retailer break our scraper," not exhaustive
correctness (that's Tier 1's job, against the frozen fixture).

Tier 2 deliberately uses **loose** assertions (`>= N`, "price is positive")
because live data is nondeterministic — Liquorland lists however many
products it lists today. Anything that needs an *exact* count or a precise
before/after state check lives in Tier 3, not here.

### Tier 3 — fixture-driven pipeline / end-to-end (offline, deterministic)

**This is the home for the two things you care most about: exact task
counts per type, and the interim state of the `drinks` table between the
listing insert and the detail-page backfill.**

Both require deterministic input. "The right number of `drink_detail`
tasks were created" only means something if the input page has a *known*
number of products — so this tier runs the **real** controller, task
queue, processors, and SQLite DB, but with a **fake fetcher** that maps
each URL to a checked-in fixture (the same fixtures Tier 1 uses). Real
pipeline, frozen input, zero network, zero credits — so we can assert
exact numbers and full-fidelity intermediate state.

Concretely, a single Liquorland run through the pipeline:

1. Seed one `page` task (mimicking `discover`) for a fixture listing URL.
2. `controller.run_next("ll", run_id)` processes it. **Assert after:**
   - `page` tasks completed = 1.
   - `drinks` table row count = number of product tiles in the fixture
     (say the fixture has exactly 24 → 24 rows).
   - Every one of those rows is in the **interim/partial** state:
     `percent = 0.0`, `stdDrinks = 0.0`, `score IS NULL`
     (`calculate_score` returns `None` when std drinks = 0), and
     `zero_alc = 0` — *not* mislabeled zero-alc despite `percent = 0`
     (this is a real invariant the processor's own cache-check relies on).
   - `drink_detail` tasks now `pending` = 24 (one per tile, since none
     were cached), and `page` follow-up tasks = 1 iff the fixture implies
     another page of results, else 0. Assert the exact split.
3. Drain the `drink_detail` tasks (`run_next` in a loop). Each maps to a
   fixture detail page via the fake fetcher. **Assert after each / at end:**
   - The corresponding `drinks` row is now **fully populated**:
     `percent > 0`, `stdDrinks > 0`, `score` recomputed and non-null,
     `zero_alc` set correctly from the parsed percent.
   - The `(store, link, pack_qty)` dedup key means the detail backfill
     *updates* the existing row rather than inserting a second one — assert
     total row count is unchanged (still 24) after all details process.
   - All `drink_detail` tasks end `completed`.

For **BWS**, the same pipeline runs but asserts the *opposite* branch:
processing a `page` task creates fully-populated rows immediately (BWS
carries `%`/std drinks in the listing JSON) and creates **zero**
`drink_detail` tasks. That contrast — LL two-phase vs BWS one-phase — is
exactly the controller's retailer-specific branch (`controller.py:234`),
so it's worth pinning both sides.

**Fake fetcher wiring (the fiddly part).** There are multiple fetch points
and they don't share one seam, so the `fake_fetchers` fixture in
`conftest.py` patches all of them at once:
- BWS + the generic path go through `Fetcher._implementation` (a class
  attribute on the singleton in `scraping/fetcher.py`) — swapped for a
  `FakeImpl(url → fixture)`.
- `LiquorlandProcessor` **bypasses** `Fetcher` entirely: its listing path
  uses `fetch_url_max_rpp` and its detail path (`get_details_from_item_page`)
  uses `fetch_url`, both building their own ScrapingBee `urllib` calls. So
  the fixture also monkeypatches `LiquorlandProcessor.fetch_url` and
  `.fetch_url_max_rpp` to the same `url → fixture` resolver.

We chose to monkeypatch in the test layer rather than refactor
`LiquorlandProcessor` to route through `self.fetcher` — the production fetch
code is deliberately left untouched (it's the sensitive, anti-ban path), and
the monkeypatch is contained entirely in `conftest.py`. A future refactor
that unifies the fetch seam would let the tests drop the LL-specific patches,
but it isn't required.

A `url → fixture` resolver (`default_resolver` in `_support.py`) maps any
BWS URL to the BWS fixture, a Liquorland URL with a numeric product-id
suffix (e.g. `_2605953`) to the detail fixture, and any other Liquorland URL
to the listing fixture. Every LL detail task therefore resolves to the same
captured detail page — fine, because Tier 3 asserts *that* rows get
populated, not their specific values.

A deterministic full-pipeline test is also the natural place to catch
concurrency regressions with *real* work: run the same seeded queue with
`num_workers > 1` and assert the end state is identical to the
single-worker run (same row count, same task counts, no double-claims) —
complementing the pure claim-race unit test in the offline queue tests.

## Batch / task queue tests (offline, Tier 1 speed)

Fully local against SQLite, no network, so these just run as normal
`pytest`. Target `db/databaseHandler.py`'s queue functions directly plus
`ScrapingController.run_next`/`discover` with a **fake processor**
(a tiny stub implementing `get_items`/`discover_tasks` with canned
return values — this is where mocking *does* make sense, since the
thing under test is the queue/controller logic, not the retailer).

Cases worth covering:
- `add_scrape_task` → `get_next_pending_task` → `update_task_status`
  round-trip; task moves `pending → in_progress → completed`.
- `_claim_next_pending_task` under concurrent claiming (spin up a few
  threads racing to claim from a small pool of pending tasks) — assert no
  task is claimed twice. This is the one place a real concurrency bug
  would hide.
- Retry path: `run_next` with a processor stub that raises → task goes
  back to `pending` (attempts < max_retries) or `failed` (attempts
  exhausted).
- `discover` → seeds the right number of tasks from a stub
  `discover_tasks` return value; respects category filtering.
- `reset_in_progress_tasks` recovers stuck `in_progress` rows.
- Liquorland-specific branch in `run_next`: a completed `page` task
  enqueues `drink_detail` follow-up tasks; BWS does not.

### DB isolation

`db/databaseBackend.py:_get_db_path()` now honours a `SQLITE_DB_PATH`
environment override (falling back to `db/database.db`):

```python
def _get_db_path():
    override = os.environ.get("SQLITE_DB_PATH")
    if override:
        return Path(override)
    return Path(__file__).parent / "database.db"
```

The `temp_db` fixture in `conftest.py` sets `SQLITE_DB_PATH` to a
per-test `tmp_path` file, forces `Config.USE_LOCAL_DB = True`, and calls
the existing `db.databaseHandler.ensure_tables(conn)` (which builds the
schema from `db/schema/tables/*.sql`) — no `scripts/init_db.py` shell-out.
Because the override is read from the environment on every
`create_connection()`, the connections the controller opens *internally*
during a pipeline test hit the same throwaway DB automatically.

## New-site processor workflow (aim #3)

The suite doubles as a build harness for adding a retailer, so you spend
~1–2 real scrapes total instead of hammering the site while you iterate:

1. **Capture once.** Add the new site's listing (and, if it's two-phase
   like LL, one detail) URL to `golden_urls.json`, run
   `refresh_fixtures.py` — one real fetch, saved to `fixtures/`. From here
   on, development is offline.
2. **Write the processor** (`newsite_processor.py`, subclass
   `RetailerProcessor`, implement `get_items` / `discover_tasks`, optional
   `build_detail_tasks`) and register it in `controller.processors`.
3. **Iterate against the fixture** (Tier 1): a `test_newsite_processor.py`
   loads the saved fixture and asserts `get_items` extracts sane
   `DrinkItem`s. Tweak selectors, re-run `pytest` in milliseconds, repeat.
   No credits burned during the loop.
4. **Wire into the pipeline** (Tier 3): add the new retailer to the
   fixture-driven pipeline test — assert its task-count shape (one-phase
   like BWS, or two-phase like LL) and interim drinks state.
5. **Confirm against reality** (Tier 2): run `pytest -m live` once to
   verify the processor works on the real site, then you're done — and
   that same live test keeps guarding against future layout changes.

This is also the point to fix the two known gaps for First Choice, whose
raw HTML dumps already sit in `temp/fc__*.html` and can seed the first
fixture: there's no `FirstChoiceProcessor` and no `fc` entry in
`controller.processors` / `sitemaps.json`, so `--store=fc` currently
`KeyError`s. The workflow above is exactly how you'd stand it up.

## File layout

See "As-built summary" at the top. `requirements.txt` gained only `pytest`
— no mocking library is needed: Tier 1/3 fixtures are plain files and the
orchestration tests use the hand-written `FakeProcessor` rather than
`unittest.mock`.

## Decisions made (were open questions during design)

1. **DB path override** — done. `_get_db_path()` honours `SQLITE_DB_PATH`;
   the `temp_db` fixture uses it (see "DB isolation").
2. **Golden URLs** — picked and captured: a BWS beer listing (small
   pageSize), the Liquorland beer listing, and an LL detail URL derived at
   capture time. Stored in `tests/scraping/golden_urls.json`.
3. **Fixture freshness** — left to a manual `refresh_fixtures.py` re-run.
   Tier 2 (`pytest -m live`) is the real staleness backstop: it fails when a
   retailer changes their layout, which is the cue to recapture. No
   mtime-based warning was added.
4. **Liquorland fetch seam** — kept the production code as-is; the
   `fake_fetchers` fixture monkeypatches `LiquorlandProcessor.fetch_url` /
   `.fetch_url_max_rpp` in the test layer (see "Fake fetcher wiring"). The
   sensitive anti-ban fetch path is left untouched.
