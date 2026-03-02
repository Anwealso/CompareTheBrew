-- Metrics table
-- Tracks user search queries and location data
CREATE TABLE IF NOT EXISTS "metrics" (
    "ID" INTEGER PRIMARY KEY AUTOINCREMENT,
    "IP" TEXT,
    "query" TEXT,
    "datetime" TEXT,
    "country" TEXT,
    "region" TEXT,
    "city" TEXT,
    "lat" REAL,
    "long" REAL,
    "hostname" TEXT,
    "org" TEXT
);
