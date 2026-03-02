import re
from typing import Optional


SYNONYMS = {
    "coke": ["coca", "cola", "coca-cola", "coke cola"],
    "cokes": ["coca", "cola", "coca-cola", "coke cola"],
    "soda": ["soft drink", "fizzy drink", "pop"],
    "sodas": ["soft drink", "fizzy drink", "pop"],
    "litre": ["l", "liter", "litre", "litres", "liters", "ltr"],
    "litre": ["l", "liter", "litre", "litres", "liters", "ltr"],
    "ml": ["millilitre", "milliliter", "millilitres", "milliliters"],
    "bottle": ["bottled", "bottles", "btl"],
    "can": ["cans", "tinny", "tinnie"],
    "pack": ["packs", "pk", "carton", "cartons", "ctn"],
    "case": ["cases", "carton", "cartons"],
    "beer": ["beers", "lagers", "ales"],
    "spirits": ["spirit", "liquor", "hard liquor"],
    "wine": ["wines"],
    "whisky": ["whiskey", "whisky", "whiskeys"],
    "vodka": ["vodkas"],
    "rum": ["rums"],
    "gin": ["gins"],
    "tequila": ["tequilas"],
    "cider": ["ciders", "cyder"],
    "premix": ["ready to drink", "rtd", "premixed", "premixed drinks"],
    "lite": ["light", "low carb", "low calorie"],
    "mid": ["mid strength", "midstrength"],
    "full": ["full strength", "full bodied"],
    "strength": ["abv", "alcohol by volume"],
    "sparkling": ["fizzy", "carbonated", "bubbly"],
    "dry": ["brut", "extra dry"],
    "sweet": ["sweet", "dessert", "late harvest"],
    "red": ["reds", "cabernet", "merlot", "shiraz", "pinot noir"],
    "white": ["whites", "chardonnay", "sauvignon blanc", "pinot gris", "riesling"],
    "rose": ["rosé", "rosado", "pink"],
    "apple": ["apples", "apple cider"],
    "lemon": ["lemonade", "citrus"],
    "lime": ["citrus"],
    "orange": ["citrus"],
    "pineapple": ["tropical"],
    "passionfruit": ["tropical"],
    "guava": ["tropical"],
    "berry": ["berries", "strawberry", "raspberry", "blueberry", "blackberry"],
    "peach": ["stone fruit"],
    "apricot": ["stone fruit"],
    "plum": ["stone fruit"],
    "coffee": ["espresso", "coffee flavour"],
    "chocolate": ["cocoa", "mocha"],
    "vanilla": ["vanilla flavour"],
    "honey": ["honeyed", "nectar"],
    "ginger": ["ginger beer", "ginger ale"],
    "spiced": ["spice", "christmas", "winter"],
    "oak": ["oaked", "barrel", "wood"],
    "vintage": ["year", "aged", "old"],
    "premium": ["premium", "luxury", "upscale", "super premium"],
    "budget": ["value", "cheap", "economy", "affordable"],
    "imported": ["import", "overseas", "international"],
    "local": ["australian", "aussie", "nz", "new zealand"],
    "craft": ["artisan", "small batch", "independent", "microbrewery"],
    "organic": ["organic", "bio", "natural"],
    "non alcoholic": ["na", "n/a", "alcohol free", "zero alcohol", "dealcoholised"],
    "gluten free": ["gf", "gluten-free"],
    "vegan": ["vegan", "plant based", "vegetarian"],
}


def tokenize(query: str) -> list[str]:
    """Tokenize query into lowercase alphanumeric tokens."""
    tokens = re.findall(r'\b[a-zA-Z0-9]+\b', query.lower())
    return [t for t in tokens if len(t) > 1]


def expand_synonyms(tokens: list[str]) -> set[str]:
    """Expand tokens using synonym dictionary."""
    expanded = set()
    for token in tokens:
        expanded.add(token)
        if token in SYNONYMS:
            expanded.update(SYNONYMS[token])
        for key, synonyms in SYNONYMS.items():
            if token in synonyms:
                expanded.add(key)
                expanded.update(synonyms)
    return expanded


def extract_size(query: str) -> Optional[float]:
    """Extract size in litres from query."""
    size_patterns = [
        r'(\d+(?:\.\d+)?)\s*(?:l|litre|litres|liter|liters|ltr)',
        r'(\d+(?:\.\d+)?)l(?:\b|$)',
        r'(?:^|\s)(\d+(?:\.\d+)?)(?:\s|$)',
    ]
    for pattern in size_patterns:
        match = re.search(pattern, query.lower())
        if match:
            return float(match.group(1))
    
    ml_match = re.search(r'(\d+)\s*(?:ml|millilitre|milliliter)', query.lower())
    if ml_match:
        return float(ml_match.group(1)) / 1000
    
    return None


def extract_pack_count(query: str) -> Optional[int]:
    """Extract pack count from query."""
    pack_patterns = [
        r'(\d+)\s*(?:pack|pk|carton|case|ctn|can|bottle|btl)s?\b',
        r'(?:^|\s)(\d+)(?:\s|$)',
    ]
    for pattern in pack_patterns:
        match = re.search(pattern, query.lower())
        if match:
            count = int(match.group(1))
            if 1 <= count <= 100:
                return count
    return None


def normalize_query(query: str) -> dict:
    """Normalize query: tokenize and expand synonyms."""
    tokens = tokenize(query)
    expanded = expand_synonyms(tokens)
    size_l = extract_size(query)
    pack_count = extract_pack_count(query)
    
    return {
        "original": query,
        "tokens": tokens,
        "expanded_tokens": expanded,
        "size_l": size_l,
        "pack_count": pack_count,
    }


def build_search_query(normalized: dict) -> tuple[str, list]:
    """Build SQL query from normalized search parameters."""
    tokens = normalized["expanded_tokens"]
    size_l = normalized["size_l"]
    pack_count = normalized["pack_count"]
    
    conditions = []
    params = []
    
    if tokens:
        token_conditions = []
        for token in tokens:
            token_conditions.append("search_text ILIKE %s")
            params.append(f"%{token}%")
        conditions.append(f"({' OR '.join(token_conditions)})")
    
    if size_l:
        size_ml = int(size_l * 1000)
        margin = int(size_ml * 0.1)
        conditions.append("size_ml BETWEEN %s AND %s")
        params.extend([size_ml - margin, size_ml + margin])
    
    if pack_count:
        conditions.append("pack_count = %s")
        params.append(pack_count)
    
    if not conditions:
        where_clause = ""
    else:
        where_clause = "WHERE " + " AND ".join(conditions)
    
    query = f"SELECT id, name, brand, category, size_ml, pack_count, price FROM products {where_clause}"
    return query, params


def calculate_score(row: dict, normalized: dict) -> float:
    """Calculate relevance score for a result."""
    score = 0.0
    tokens = normalized["expanded_tokens"]
    name = row.get("name", "").lower()
    brand = row.get("brand", "").lower()
    category = row.get("category", "").lower()
    search_text = row.get("search_text", "").lower()
    
    if normalized["tokens"]:
        for token in normalized["tokens"]:
            if token in name:
                score += 5
            if token in brand:
                score += 3
            if token in category:
                score += 2
    
    if normalized["size_l"]:
        size_ml = row.get("size_ml", 0)
        target_ml = int(normalized["size_l"] * 1000)
        if size_ml:
            size_diff = abs(size_ml - target_ml)
            if size_diff == 0:
                score += 4
            elif size_diff <= target_ml * 0.05:
                score += 3
            elif size_diff <= target_ml * 0.1:
                score += 2
    
    if normalized["pack_count"]:
        if row.get("pack_count") == normalized["pack_count"]:
            score += 3
    
    token_count = sum(1 for t in tokens if t in search_text)
    score += token_count * 0.5
    
    return score


def rank_results(rows: list[dict], normalized: dict) -> list[tuple]:
    """Rank results by calculated score."""
    scored = [(row, calculate_score(row, normalized)) for row in rows]
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored


def intellisearch(query: str, db_connection=None) -> list[dict]:
    """
    Perform intelligent search with SQL + Normalized Tokens + Synonym Expansion + Ranking.
    
    Args:
        query: Search query string
        db_connection: Optional database connection (psycopg2 compatible)
    
    Returns:
        List of ranked product dictionaries
    """
    normalized = normalize_query(query)
    
    sql_query, params = build_search_query(normalized)
    
    if db_connection:
        cursor = db_connection.cursor()
        cursor.execute(sql_query, params)
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    else:
        rows = []
    
    for row in rows:
        search_parts = [
            row.get("name", ""),
            row.get("brand", ""),
            row.get("category", ""),
        ]
        row["search_text"] = " ".join(search_parts).lower()
    
    ranked = rank_results(rows, normalized)
    return [row for row, score in ranked]


def build_search_text(name: str, brand: str, category: str, packaging: str = "") -> str:
    """Build denormalized search_text field from product components."""
    parts = [name, brand, category, packaging]
    return " ".join(str(p).lower() for p in parts if p)


def main():
    """CLI for testing intellisearch."""
    print("Enter search query:")
    query = input()
    results = intellisearch(query)
    
    print(f"\nNormalized query: {normalize_query(query)}")
    print(f"\nSQL: {build_search_query(normalize_query(query))[0]}")
    print(f"\nResults ({len(results)}):")
    for i, r in enumerate(results[:10], 1):
        print(f"  {i}. {r.get('name', 'N/A')} - {r.get('brand', 'N/A')}")


if __name__ == "__main__":
    main()
