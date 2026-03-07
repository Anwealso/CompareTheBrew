-- Scrape Tasks table
-- Stores the queue of tasks for the scraping engine
CREATE TABLE IF NOT EXISTS "scrape_tasks" (
    "ID" INTEGER PRIMARY KEY AUTOINCREMENT,
    "retailer" TEXT NOT NULL,
    "url" TEXT NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'pending',
    "metadata" TEXT,
    "run_id" TEXT,
    "attempts" INTEGER DEFAULT 0,
    "created_at" TEXT NOT NULL,
    "updated_at" TEXT NOT NULL,
    FOREIGN KEY ("run_id") REFERENCES "runs"("uuid")
);

-- Indices for faster querying of pending tasks
CREATE INDEX IF NOT EXISTS idx_tasks_retailer_status ON scrape_tasks(retailer, status);
CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON scrape_tasks(created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_run_id ON scrape_tasks(run_id);
