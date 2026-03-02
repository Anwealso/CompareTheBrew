-- Schema version table
-- Tracks applied schema migrations
CREATE TABLE IF NOT EXISTS "schema_version" (
    "ID" INTEGER PRIMARY KEY AUTOINCREMENT,
    "version" INTEGER NOT NULL,
    "applied_at" TEXT DEFAULT CURRENT_TIMESTAMP
);
