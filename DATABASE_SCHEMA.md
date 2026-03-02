# Database Schema

## Current Schema (v1.1)

### drinks table

| Column | Type | Description |
|--------|------|-------------|
| ID | INTEGER | Primary key, auto-increment |
| store | TEXT | Store identifier (e.g., 'bws', 'danmurphys') |
| brand | BLOB | Brand name |
| name | NUMERIC | Product name |
| type | TEXT | Product category/type |
| price | NUMERIC | Price in AUD |
| link | TEXT | Product URL |
| ml | REAL | Volume in milliliters |
| percent | REAL | Alcohol percentage (ABV) |
| stdDrinks | REAL | Standard drinks count |
| efficiency | REAL | stdDrinks / price (value metric) |
| image | TEXT | Full image URL |
| shortimage | TEXT | Shortened image identifier |
| search_text | TEXT | Denormalized search text |

### metrics table

| Column | Type | Description |
|--------|------|-------------|
| ID | INTEGER | Primary key, auto-increment |
| IP | TEXT | Client IP address |
| query | TEXT | Search query |
| datetime | TEXT | Timestamp of query |
| country | TEXT | Client country |
| region | TEXT | Client region |
| city | TEXT | Client city |
| lat | REAL | Latitude |
| long | REAL | Longitude |
| hostname | TEXT | Hostname |
| org | TEXT | Organization |

### sources table

| Column | Type | Description |
|--------|------|-------------|
| ID | INTEGER | Primary key, auto-increment |
| url | TEXT | Source URL (unique) |
| retailer | TEXT | Retailer identifier |
| last_scraped | TEXT | Timestamp of last scrape |

---

## Migrations

### v1.1 - Add search_text for intellisearch (APPLIED)

Adds a denormalized text column for efficient text searching.

```sql
-- Add search_text column
ALTER TABLE drinks ADD COLUMN search_text TEXT;

-- Populate search_text from existing data
UPDATE drinks 
SET search_text = LOWER(
    COALESCE(CAST(name AS TEXT), '') || ' ' || 
    COALESCE(CAST(brand AS TEXT), '') || ' ' || 
    COALESCE(type, '') || ' ' ||
    COALESCE(store, '')
);

-- Create index for faster searches
CREATE INDEX idx_drinks_search_text ON drinks(search_text);
```

### v1.2 - Add sources table (APPLIED)

Tracks source URLs for scraping.

```sql
CREATE TABLE IF NOT EXISTS "sources" (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT UNIQUE,
    retailer TEXT,
    last_scraped TEXT
);
```

### Future Considerations

- Add `pack_count` column for multi-pack products
- Add `packaging_format` column (bottle, can, carton, etc.)
- Add `is_active` boolean column for soft-delete
- Add foreign key from drinks to sources
