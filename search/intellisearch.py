import json
import re
from pathlib import Path
from typing import Optional

SYNONYMS = json.loads((Path(__file__).parent / "search_synonyms.json").read_text())

try:
    from rapidfuzz import fuzz
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False


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

def get_additional_quality_filters(
    *,
    text_fields: list[str] | None = None,
    price_field: str = "price",
    std_field: str = "std_drinks",
    size_field: str | None = "size_ml",
) -> list[str]:
    """Return SQL predicates that skip broken/empty product rows."""
    if text_fields is None:
        text_fields = ["name", "brand", "category", "search_text"]

    conditions: list[str] = []
    for field in text_fields:
        conditions.append(f"{field} IS NOT NULL AND TRIM({field}) <> ''")

    conditions.append(f"{price_field} IS NOT NULL AND {price_field} > 0")
    conditions.append(f"{std_field} IS NOT NULL AND {std_field} > 0")
    if size_field:
        conditions.append(f"{size_field} IS NOT NULL AND {size_field} > 0")

    return conditions

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
        # expanded / synonyms results are OR'ed together so we fetch anything
        # that might reasonably match the user's intent before ranking later.

    if size_l:
        size_ml = int(size_l * 1000)
        margin = int(size_ml * 0.1)
        conditions.append("size_ml BETWEEN %s AND %s")
        params.extend([size_ml - margin, size_ml + margin])

    if pack_count:
        conditions.append("pack_count = %s")
        params.append(pack_count)

    conditions.extend(get_additional_quality_filters())

    if not conditions:
        where_clause = ""
    else:
        where_clause = "WHERE " + " AND ".join(conditions)

    query = f"SELECT id, name, brand, category, size_ml, pack_count, price FROM products {where_clause}"
    return query, params


def fuzz_score(query: str, target: str) -> float:
    """
    Calculate fuzzy match score between query and target string.
    Uses rapidfuzz if available, returns 0 otherwise.
    """
    if not RAPIDFUZZ_AVAILABLE or not query or not target:
        return 0.0
    
    partial = fuzz.partial_ratio(query.lower(), target.lower())
    token_sort = fuzz.token_sort_ratio(query.lower(), target.lower())
    
    return max(partial, token_sort) / 100.0


def calculate_score(row: dict, normalized: dict) -> float:
    """Calculate relevance score for a result."""
    score = 0.0
    query_tokens = normalized["tokens"]
    name = row.get("name", "").lower()
    brand = row.get("brand", "").lower()
    category = row.get("category", "").lower()
    search_text = row.get("search_text", "").lower()
    original_query = normalized["original"]
    normalized_query = original_query.lower().strip()

    if query_tokens:
        for token in query_tokens:
            if token in brand:
                score += 5  # prefer hits where the query token is embedded in the brand name
            if token in name:
                score += 3  # next priority: token in product name
            if token in category:
                score += 2  # minor boost if it appears in the category label

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

    unique_query_tokens = set(query_tokens)
    keyword_matches = 0
    if unique_query_tokens:
        keyword_matches = sum(1 for t in unique_query_tokens if t in search_text)
        score += keyword_matches * 2  # reward each matching keyword
        if keyword_matches == len(unique_query_tokens):
            score += 2  # bonus for covering the entire query perfectly

    if normalized_query and normalized_query in search_text:
        score += 3  # strong boost for exact phrase match

    if RAPIDFUZZ_AVAILABLE:
        score += fuzz_score(original_query, name) * 3
        score += fuzz_score(original_query, search_text) * 1.5

    return score


def rank_results(rows: list[dict], normalized: dict) -> list[tuple]:
    """Rank results by keyword overlap first, then by calculated score."""
    query_tokens = normalized["tokens"]
    unique_tokens = set(query_tokens)
    scored = []
    for row in rows:
        score = calculate_score(row, normalized)
        search_text = row.get("search_text", "").lower()
        keyword_matches = (
            sum(1 for token in unique_tokens if token in search_text)
            if unique_tokens
            else 0
        )
        # track both total keyword overlap and the underlying score so we can
        # sort by overlap first and fall back to the richer score second
        scored.append((row, keyword_matches, score))

    scored.sort(key=lambda entry: (entry[1], entry[2]), reverse=True)
    return [(row, score) for row, _, score in scored]


def get_expanded_terms(query: str) -> list[str]:
    """
    Get expanded search terms from a query using synonym expansion.
    Useful when you just need token expansion without running SQL.
    
    Args:
        query: Search query string
    
    Returns:
        List of expanded search terms
    """
    normalized = normalize_query(query)
    return list(normalized["expanded_tokens"])


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
    
    # If no DB connection, return expanded terms for use in other queries
    if not db_connection:
        sql_query, params = build_search_query(normalized)
        return list(normalized["expanded_tokens"])
    
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
