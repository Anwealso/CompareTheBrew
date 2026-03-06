-- Drinks table
-- Main product table with search_text for intellisearch
CREATE TABLE IF NOT EXISTS "drinks" (
    "ID" INTEGER PRIMARY KEY AUTOINCREMENT,
    "store" TEXT,
    "brand" BLOB,
    "name" NUMERIC,
    "type" TEXT,
    "price" REAL,
    "link" TEXT,
    "ml" REAL,
    "percent" REAL,
    "stdDrinks" REAL,
    "efficiency" REAL,
    "image" TEXT,
    "shortimage" TEXT,
    "search_text" TEXT,
    "location" TEXT,
    "date_created" TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for drinks table
CREATE INDEX IF NOT EXISTS idx_drinks_search_text ON drinks(search_text);
CREATE INDEX IF NOT EXISTS idx_drinks_store ON drinks(store);
CREATE INDEX IF NOT EXISTS idx_drinks_type ON drinks(type);
CREATE INDEX IF NOT EXISTS idx_drinks_efficiency ON drinks(efficiency);
CREATE INDEX IF NOT EXISTS idx_drinks_price ON drinks(price);
CREATE INDEX IF NOT EXISTS idx_drinks_ml ON drinks(ml);
