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
    "pack_qty" INTEGER DEFAULT 1,
    "ml" REAL,
    "percent" REAL,
    "stdDrinks" REAL,
    "score" REAL,
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
CREATE INDEX IF NOT EXISTS idx_drinks_score ON drinks(score);
CREATE INDEX IF NOT EXISTS idx_drinks_price ON drinks(price);
CREATE INDEX IF NOT EXISTS idx_drinks_ml ON drinks(ml);
CREATE INDEX IF NOT EXISTS idx_drinks_store_link_pack_qty ON drinks(store, link, pack_qty);
