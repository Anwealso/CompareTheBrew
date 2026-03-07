import argparse
import shutil
import sqlite3
import sys
import textwrap
from pathlib import Path

def get_db_path():
    return Path(__file__).parent.parent / "db" / "database.db"

def create_connection():
    db_path = get_db_path()
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}")
        return None
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def get_task_stats(conn, retailer=None):
    cur = conn.cursor()
    
    if retailer:
        cur.execute("""
            SELECT status, COUNT(*) as count 
            FROM scrape_tasks 
            WHERE retailer = ?
            GROUP BY status
        """, (retailer,))
    else:
        cur.execute("""
            SELECT status, COUNT(*) as count 
            FROM scrape_tasks 
            GROUP BY status
        """)
    
    return cur.fetchall()

def get_tasks_by_retailer(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT retailer, status, COUNT(*) as count 
        FROM scrape_tasks 
        GROUP BY retailer, status
        ORDER BY retailer, status
    """)
    return cur.fetchall()

def get_pending_tasks(conn, retailer=None, limit=20):
    cur = conn.cursor()
    
    if retailer:
        cur.execute("""
            SELECT ID, retailer, url, status, attempts, created_at, updated_at
            FROM scrape_tasks 
            WHERE retailer = ? AND status = 'pending'
            ORDER BY created_at ASC
            LIMIT ?
        """, (retailer, limit))
    else:
        cur.execute("""
            SELECT ID, retailer, url, status, attempts, created_at, updated_at
            FROM scrape_tasks 
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT ?
        """, (limit,))
    
    return cur.fetchall()

def get_recent_tasks(conn, status=None, limit=20):
    cur = conn.cursor()
    
    if status:
        cur.execute("""
            SELECT ID, retailer, url, status, attempts, created_at, updated_at
            FROM scrape_tasks 
            WHERE status = ?
            ORDER BY updated_at DESC
            LIMIT ?
        """, (status, limit))
    else:
        cur.execute("""
            SELECT ID, retailer, url, status, attempts, created_at, updated_at
            FROM scrape_tasks 
            ORDER BY updated_at DESC
            LIMIT ?
        """, (limit,))
    
    return cur.fetchall()

def main():
    parser = argparse.ArgumentParser(description='CompareTheBrew Task Queue CLI')
    parser.add_argument('--retailer', type=str, help='Filter by retailer (e.g., bws, danmurphys)')
    parser.add_argument('--status', type=str, choices=['pending', 'in_progress', 'completed', 'failed'], 
                        help='Filter by status')
    parser.add_argument('--limit', type=int, default=20, help='Number of tasks to show (default: 20)')
    parser.add_argument('--show-pending', action='store_true', help='Show pending tasks')
    parser.add_argument('--show-stats', action='store_true', help='Show task statistics')
    
    args = parser.parse_args()

    conn = create_connection()
    if not conn:
        sys.exit(1)

    try:
        if args.show_stats:
            print("\n=== Task Queue Statistics ===\n")
            
            if args.retailer:
                stats = get_task_stats(conn, args.retailer)
                print(f"Tasks for retailer: {args.retailer}")
                print("-" * 40)
                total = 0
                for status, count in stats:
                    print(f"  {status:<15}: {count:>5}")
                    total += count
                print("-" * 40)
                print(f"  {'Total':<15}: {total:>5}")
            else:
                stats = get_task_stats(conn)
                print("All tasks by status:")
                print("-" * 40)
                total = 0
                for status, count in stats:
                    print(f"  {status:<15}: {count:>5}")
                    total += count
                print("-" * 40)
                print(f"  {'Total':<15}: {total:>5}")
            
            print("\nTasks by retailer and status:")
            print("-" * 60)
            retailer_stats = get_tasks_by_retailer(conn)
            current_retailer = None
            for retailer, status, count in retailer_stats:
                if retailer != current_retailer:
                    if current_retailer is not None:
                        print()
                    print(f"{retailer}:")
                    current_retailer = retailer
                print(f"  {status:<15}: {count:>5}")
            print("-" * 60)
            
        elif args.show_pending:
            print(f"\n=== Pending Tasks ===\n")
            tasks = get_pending_tasks(conn, args.retailer, args.limit)
            
            if not tasks:
                print("No pending tasks found.")
                return
            
            url_col_width = 60
            
            print(f"{'ID':<5} | {'Retailer':<15} | {'URL':<60} | {'Attempts':<8}")
            print("-" * 98)
            
            for row in tasks:
                task_id, retailer, url, status, attempts, created_at, updated_at = row
                lines = [url[i:i+60] for i in range(0, len(url), 60)]
                print(f"{task_id:<5} | {retailer:<15} | {lines[0]:<60} | {attempts:<8}")
                for line in lines[1:]:
                    print(f"{'':5} | {'':<15} | {line:<60} | {'':<8}")
                
                
        else:
            print(f"\n=== Recent Tasks ===")
            if args.status:
                print(f" (filtered by status: {args.status})")
            elif args.retailer:
                print(f" (filtered by retailer: {args.retailer})")
            print()
            
            if args.status:
                tasks = get_recent_tasks(conn, args.status, args.limit)
            elif args.retailer:
                cur = conn.cursor()
                cur.execute("""
                    SELECT ID, retailer, url, status, attempts, created_at, updated_at
                    FROM scrape_tasks 
                    WHERE retailer = ?
                    ORDER BY updated_at DESC
                    LIMIT ?
                """, (args.retailer, args.limit))
                tasks = cur.fetchall()
            else:
                tasks = get_recent_tasks(conn, None, args.limit)
            
            if not tasks:
                print("No tasks found.")
                return
            
            url_col_width = 60
            
            print(f"{'ID':<5} | {'Retailer':<12} | {'Status':<12} | {'URL':<60} | {'Attempts':<8}")
            print("-" * 105)
            
            for row in tasks:
                task_id, retailer, url, status, attempts, created_at, updated_at = row
                lines = [url[i:i+60] for i in range(0, len(url), 60)]
                print(f"{task_id:<5} | {retailer:<12} | {status:<12} | {lines[0]:<60} | {attempts:<8}")
                for line in lines[1:]:
                    print(f"{'':5} | {'':<12} | {'':<12} | {line:<60} | {'':<8}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
