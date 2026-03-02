-- Auto-generated schema file - DO NOT EDIT MANUALLY
-- Run 'python3 scripts/sync_schema.py' to update

CREATE TABLE "drinks" (
	`ID`	INTEGER PRIMARY KEY AUTOINCREMENT,
	`store`	TEXT,
	`brand`	BLOB,
	`name`	NUMERIC,
	`type`	TEXT,
	`price`	NUMERIC,
	`link`	TEXT,
	`ml`	REAL,
	`percent`	REAL,
	`stdDrinks`	REAL,
	`efficiency`	REAL,
	`image`	TEXT,
	`shortimage`	TEXT
, search_text TEXT);

CREATE TABLE "metrics" ( `ID` INTEGER PRIMARY KEY AUTOINCREMENT, `IP` TEXT,
            `query` TEXT, `datetime` TEXT, `country` TEXT, `region` TEXT, `city` TEXT, `lat` REAL, `long` REAL,
            `hostname` TEXT, `org` TEXT);

CREATE TABLE "sources" (
            `ID` INTEGER PRIMARY KEY AUTOINCREMENT,
            `url` TEXT UNIQUE,
            `retailer` TEXT,
            `last_scraped` TEXT
        );

CREATE INDEX idx_drinks_search_text ON drinks(search_text);

