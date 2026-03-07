-- Runs table
-- Stores metadata about each scraping run
CREATE TABLE IF NOT EXISTS "runs" (
    "uuid" TEXT PRIMARY KEY,
    "start_time" TEXT NOT NULL,
    "end_time" TEXT,
    "status" TEXT DEFAULT 'in_progress',
    "retailer" TEXT,
    "category" TEXT
);

-- Index for querying runs
CREATE INDEX IF NOT EXISTS idx_runs_start_time ON runs(start_time);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
