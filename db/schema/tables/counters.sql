-- Metrics table
-- Stores metric counters with optional key-based tracking
CREATE TABLE IF NOT EXISTS "metrics" (
    "ID" INTEGER PRIMARY KEY AUTOINCREMENT,
    "metric_name" TEXT NOT NULL,
    "key" TEXT,
    "value" INTEGER DEFAULT 0,
    UNIQUE(metric_name, key)
);