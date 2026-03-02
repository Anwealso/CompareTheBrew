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
- `SCRAPING_API_KEY`: Key for third-party scraping services (if needed).
- `IPINFO_TOKEN`: Token for location-based search tracking.

## Usage

### Virtual Environment
Activate the environment before running any scripts:
```bash
$ source venv/bin/activate
```

### 1. Scraping
Scrape drinks from a specific store and category:
```bash
$ python3 scrape.py [store] [category]
```
- **store**: `bws` (others currently under development)
- **category**: `beer`, `wine`, `spirits`, or a specific search term.

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
Search for drinks directly from your terminal:
```bash
$ python3 db_search.py whiskey --sort efficiency --order DESC --limit 10
```
- **--sort**: `efficiency`, `price`, `percent`, `ml` (default: efficiency)
- **--order**: `ASC`, `DESC` (default: DESC)
- **--limit**: Number of results to show (default: 10)

## API Queries
The server provides a search API:
`http://localhost:5000/api?term=TERM&order=score_desc`

## Dependencies
- BeautifulSoup4
- Flask
- Python-dotenv
- IPInfo
- SQLite3

___
### Made Possible by:
Alex Nicholson | Hamish Bultitude | Matt Costello
