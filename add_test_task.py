from db.databaseHandler import create_connection, add_scrape_task
import json

def add_single_test_task():
    conn = create_connection()
    if not conn:
        print("Failed to connect to DB")
        return
    
    retailer = "ll"
    url = "https://www.liquorland.com.au/api/v1/search/wine"
    metadata = {"page": 1, "test": True}
    
    add_scrape_task(conn, retailer, url, metadata)
    print(f"Added single task for {retailer}: {url}")
    conn.close()

if __name__ == "__main__":
    add_single_test_task()
