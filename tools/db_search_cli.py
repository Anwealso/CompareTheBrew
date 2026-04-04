import argparse
import sys
from db.databaseHandler import create_connection, select_drink_by_smart_search

def main():
    parser = argparse.ArgumentParser(description='CompareTheBrew Database Search CLI')
    parser.add_argument('terms', type=str, nargs='+', help='Search terms (e.g. "whiskey", "vodka party")')
    parser.add_argument('--sort', choices=['efficiency', 'price', 'percent', 'ml'], default='efficiency',
                        help='Category to sort by (default: efficiency)')
    parser.add_argument('--order', choices=['ASC', 'DESC'], default='DESC',
                        help='Sort order (default: DESC)')
    parser.add_argument('--limit', type=int, default=10, help='Number of results to show (default: 10)')

    args = parser.parse_args()

    # Join terms into a single string as expected by select_drink_by_smart_search
    search_terms = " ".join(args.terms)
    sort_param = f"{args.order}_{args.sort}"

    conn = create_connection()
    if not conn:
        print("Error: Could not connect to database.")
        sys.exit(1)

    try:
        results = select_drink_by_smart_search(conn, search_terms, sort_param)
        
        if not results:
            print(f"No results found for '{search_terms}'.")
            return

        print(f"\nFound {len(results)} results. Showing top {min(args.limit, len(results))}:")
        print("-" * 100)
        header = f"{'Store':<10} | {'Brand':<20} | {'Name':<40} | {'Price':<8} | {'Efficiency':<10}"
        print(header)
        print("-" * 100)

        for i, row in enumerate(results[:args.limit]):
            # Row mapping based on table schema in create_connection:
            # 0:ID, 1:store, 2:brand, 3:name, 4:type, 5:price, 6:link, 7:pack_qty, 8:ml, 9:percent, 10:stdDrinks, 11:efficiency, 12:image, 13:shortimage
            store = str(row[1])
            brand = str(row[2])
            name = str(row[3])
            price = f"${row[5]:.2f}"
            efficiency = f"{row[11]:.4f}"
            
            # Truncate long strings for display
            brand = (brand[:17] + '...') if len(brand) > 20 else brand
            name = (name[:37] + '...') if len(name) > 40 else name
            
            print(f"{store:<10} | {brand:<20} | {name:<40} | {price:<8} | {efficiency:<10}")
            
    finally:
        conn.close()

if __name__ == "__main__":
    main()
