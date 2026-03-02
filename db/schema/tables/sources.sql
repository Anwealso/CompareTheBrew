-- Sources table
-- Tracks source URLs for scraping
CREATE TABLE IF NOT EXISTS "sources" (
    "ID" INTEGER PRIMARY KEY AUTOINCREMENT,
    "url" TEXT UNIQUE,
    "retailer" TEXT,
    "last_scraped" TEXT
);

-- Indexes for sources table
CREATE INDEX IF NOT EXISTS idx_sources_retailer ON sources(retailer);
