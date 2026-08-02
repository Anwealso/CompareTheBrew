# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

CompareTheBrew2 is an Australian alcohol price comparison site. It scrapes BWS, Liquor Land, and First Choice, stores drink data in a database, and serves search results via a Flask web app. The "score" metric (price per standard drink) is the core ranking signal.

## Commands

### Setup
```bash
bash setup.sh          # Creates venv and installs dependencies
cp .env.example .env   # Then fill in API keys
source venv/bin/activate
python3 scripts/init_db.py  # Initialize database schema
```

### Run
```bash
python3 app.py                        # Dev server on localhost:5000
gunicorn -c gunicorn.conf.py wsgi:app # Production
```

### Scraping
```bash
python3 tools/scraping-controller-cli.py --store=ll --new --workers=2
# --store: bws | ll | fc | all
# --category: beer | wine | spirits | premix
# --new: discover + scrape fresh; --continue: resume pending tasks
```

### Database utilities
```bash
python3 scripts/recalc_scores.py       # Recalculate score for all drinks
python3 scripts/migrate_to_supabase.py # Migrate SQLite → Supabase
python3 tools/task_queue_cli.py --show-stats
python3 tools/db_search_cli.py whiskey 2l
```

### Testing
```bash
pytest                        # offline scraping suite (no network, no credits)
pytest -m live                # opt-in live tests via ScrapingBee (spends credits)
python tests/scraping/refresh_fixtures.py       # recapture all fixtures via ScrapingBee
python tests/scraping/refresh_fixtures.py ll    # recapture just Liquorland
```
The scraping test suite lives in `tests/scraping/`; full design and
methodology are in `docs/TESTING_DESIGN.md`. Live tests are excluded by
default (`pyproject.toml` `addopts = "-m 'not live'"`) so plain `pytest`
never touches the network or spends credits.

### Formatting
```bash
bash scripts/lint.sh   # Black, line-length 79
```

## Architecture

### Database layer
`db/databaseBackend.py` abstracts over SQLite (local dev) and PostgreSQL (Supabase production). Switch with `USE_LOCAL_DB=true/false` in `.env`. All schema is in `db/schema/tables/*.sql` — these are authoritative.

`db/databaseHandler.py` provides the high-level query interface used by the rest of the app (search, inserts, task queue operations).

### Scraping pipeline
The scraping system uses an **in-database task queue** (`scrape_tasks` table) — no external message broker. The controller (`scraping/controller.py`) orchestrates workers that atomically claim tasks, fetch URLs via `scraping/fetcher.py` (which wraps ScrapingBee), and parse results with retailer-specific processors (`bws_processor.py`, `liquorland_processor.py`). Each processor extends the abstract `RetailerProcessor` in `processor.py`.

### Scraping test methodology
The suite (`tests/scraping/`, see `docs/TESTING_DESIGN.md`) splits on the one boundary with external unpredictability — **fetch + parse**. Only that seam needs live testing (Tier 2, `@pytest.mark.live`); everything inside (queue, controller orchestration, DB writes/dedup) is deterministic and tested offline against real responses captured once as fixtures. Tier 1 tests processor parsing on those fixtures; Tier 3 drives the *real* controller + queue + DB against fixture-replay fetchers (`fake_fetchers` in `conftest.py`) to assert exact task counts and the interim vs fully-populated `drinks` state across BWS's one-phase and Liquorland's two-phase scrapes. `refresh_fixtures.py` recaptures fixtures via ScrapingBee. DB isolation is via the `SQLITE_DB_PATH` override + the `temp_db` fixture.

### Search
`search/intellisearch.py` handles fuzzy matching (RapidFuzz), synonym expansion (`search/search_synonyms.json`), and size/pack parsing. The `search_text` column in `drinks` is the indexed target for all searches.

### Metrics/observability
`observability/metrics.py` defines `Metric` (single value) and `ListMetric` (keyed, e.g. per-keyword) classes, persisted to the `metrics` table. Logs rotate daily using Sydney timezone (`observability/logging.py`). Admin dashboard at `/admin/metrics`, health check at `/healthz`.

### Flask app
`app.py` handles all routes. Key endpoints: `/` (home), `/search` (main search), `/top50`, `/api?term=...` (JSON search API). Template rendering is server-side Jinja2 with custom filters for data freshness.

## Environment variables

| Variable | Purpose |
|---|---|
| `USE_LOCAL_DB` | `true` = SQLite, `false` = Supabase PostgreSQL |
| `SUPABASE_DB_URL` | PostgreSQL connection string |
| `SCRAPINGBEE_API_KEY` | Required for scraping anti-bot retailers |
| `IPINFO_TOKEN` | Geolocation for request tracking |

See `.env.example` for the full list including Bright Data credentials.

## Key conventions

- **Score** = price / standard drinks — lower is better value. Recalculate with `recalc_scores.py` after schema changes.
- **Pack quantity** matters for deduplication: the unique key for drinks is `(store, link, pack_qty)`.
- **Composite indexes** on `drinks` cover the common filter patterns (store, type, score, price, ml).
- Black formatting at line-length 79 is enforced — run `bash scripts/lint.sh` before committing.
- The scraping guidelines in `scraping/SCRAPING_GUIDELINES.md` explain ScrapingBee config, CAPTCHA handling, and cost control — read before modifying the fetcher.
