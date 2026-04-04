# CompareTheBrew.me
🍺🍺 Get the cheapest drink possible across all Aussie alcoholic vendors 🍺🍺

## Setup & Installation

### 1. Initial Setup
Create a virtual environment and install dependencies:
```bash
$ bash setup.sh
```
This script will:
- Create a `venv` directory.
- Install all required Python packages.
- Optionally copy ChromeWebDriver to your path.

### 2. Environment Variables
Copy the example environment file and fill in your API keys:
```bash
$ cp .env.example .env
```
Edit `.env` to include your:
- `SCRAPINGBEE_API_KEY`: Key for third-party scraping services (if needed).
- `IPINFO_TOKEN`: Token for location-based search tracking.

## Usage

### Virtual Environment
Activate the environment before running any scripts:
```bash
$ source venv/bin/activate
```

### 1. Scraping (via CLI)
The preferred way to run scraping workloads is with the dedicated CLI wrapper, which keeps tasks queued, workers coordinated, and progress streamed explicitly back to your terminal. Example:
```bash
$ python3 tools/scraping-controller-cli.py --store=ll --category=wine --limit=3 --workers=2
```

Available flags:
- `--store=STORE`: Which retailer to scrape (`bws`, `ll`, `fc`, or `all`)
- `--category=CAT`: Filter discovery to `beer`, `wine`, `spirits`, or `premix`
- `--limit=N`: Limit the number of tasks processed before the run stops
- `--workers=N`: Number of worker threads to consume the queue (default: 1)
- `--discover`: Seed the queue with fresh tasks before processing
- `--new`: Start a new run (implies discovery)
- `--continue`: Continue from the existing queue tasks (default unless `--new`)
- `--resume-last`: Resume the last run for the chosen retailer/category
- `--man`: Print the full man-style page (also available via `--help`)

**Examples:**
```bash
# Full scrape with discovery and processing (sequential)
$ python3 tools/scraping-controller-cli.py --store=ll --new

# Discover only beer category and limit to three page tasks
$ python3 tools/scraping-controller-cli.py --store=bws --category=beer --discover --limit=3

# Parallel processing with 8 workers
$ python3 tools/scraping-controller-cli.py --store=fc --discover --workers=8

# Resume the latest pending run for wine
$ python3 tools/scraping-controller-cli.py --store=ll --category=wine --continue --limit=5

# Show the CLI man page
$ python3 tools/scraping-controller-cli.py --man
```

See `scraping/SCRAPING_GUIDELINES.md` for implementation details and processor-specific guidance.

### 2. Web Server
Launch the Flask application locally:
```bash
$ python3 app.py
```
Or use the provided management scripts:
- `./startserver.sh`: Starts the server using the virtual environment.
- `./seeserver.sh`: Lists active server processes.
- `./killserver.sh`: Stops the server.

### 3. Database Search CLI
Search for drinks directly from your terminal using the intelligent search system:

```bash
$ python3 db_search_cli.py whiskey 2l
$ python3 db_search_cli.py vodka --sort score --order ASC --limit 10
$ python3 db_search_cli.py "coke" "rum" --sort price --limit 5
```

#### Arguments
- **terms**: One or more search terms (e.g., "whiskey", "vodka party", "coke rum")
- **--sort**: Sort by `score` (price per standard drink), `price`, `percent`, or `ml` (default: score)
- **--order**: Sort order `ASC` or `DESC` (default: DESC)
- **--limit**: Number of results to display (default: 10)

#### How It Works
The search uses:
- **Tokenization**: Splits queries into searchable tokens
- **Synonym Expansion**: Matches common terms (e.g., "coke" → "cola", "coca-cola")
- **Size Extraction**: Parses sizes (e.g., "2l", "500ml") for precise filtering
- **SQL Query**: Searches the denormalized `search_text` column
- **Ranking**: Scores results by name match (+5), brand match (+3), size match (+4), fuzzy match
- **Fuzzy Matching**: Uses rapidfuzz for typo tolerance

#### Examples
```bash
# Find whiskey with the lowest $/Std
python3 db_search_cli.py whiskey --sort score --order ASC

# Search for specific size
python3 db_search_cli.py "2l" "beer"

# Combine terms
python3 db_search_cli.py vodka lime --sort price
```

### 4. Task Queue CLI
View the current state of the scraping task queue in the database:

```bash
$ python3 tools/task_queue_cli.py --show-stats
$ python3 tools/task_queue_cli.py --show-pending
$ python3 tools/task_queue_cli.py --retailer bws
```

#### Arguments
- **--retailer**: Filter by retailer (e.g., `bws`, `danmurphys`, `fc`, `ll`)
- **--status**: Filter by status (`pending`, `in_progress`, `completed`, `failed`)
- **--limit**: Number of tasks to show (default: 20)
- **--show-stats**: Show task statistics (counts by status and retailer)
- **--show-pending**: Show only pending tasks

#### Examples
```bash
# Show overall task queue statistics
python3 tools/task_queue_cli.py --show-stats

# Show stats for a specific retailer
python3 tools/task_queue_cli.py --show-stats --retailer bws

# Show pending tasks only
python3 tools/task_queue_cli.py --show-pending

# Show recent tasks for a retailer
python3 tools/task_queue_cli.py --retailer bws

# Show failed tasks
python3 tools/task_queue_cli.py --status failed
```

## API Queries
The server provides a search API:
`http://localhost:5000/api?term=TERM&order=score_desc`

## Database Management

### Initialize New Database
```bash
# Create fresh database with all tables and indexes
python3 scripts/init_db.py
```

### Database Schema
The SQL files in `db/schema/tables/` are the authoritative source. Edit them to make schema changes.

```bash
# Initialize database from schema
python3 scripts/init_db.py
```

The `drinks` table now records a `pack_qty` value for each row so different packaging formats (single, 6-pack, cartons, etc.) that share the same URL remain distinct. Indexes and dedup logic also key on `(store, link, pack_qty)` to avoid dropping valid variants.

Duplicate cleanup via `scripts/dedup_drinks_by_link.py` respects `pack_qty` by grouping on `(store, link, pack_qty)` before keeping the most recent/best row.

### Viewing Schema
- Authoritative SQL: See `db/schema/tables/`

## Dependencies
- BeautifulSoup4
- Flask
- Python-dotenv
- IPInfo
- SQLite3
- Black (for code formatting)

## Code Formatting
This project uses [Black](https://github.com/psf/black) for Python code formatting.

To format the codebase, run:
```bash
$ bash scripts/lint.sh
```
Config for Black is located in `pyproject.toml`.

___
### Made Possible by:
Alex Nicholson | Hamish Bultitude | Matt Costello
