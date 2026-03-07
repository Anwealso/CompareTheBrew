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
    
    # Join with runs to order by run start time
    # Status order: completed=0, in_progress=1, pending=2, failed=3 (so completed shows first)
    if retailer:
        cur.execute("""
            SELECT t.ID, t.retailer, t.url, t.status, t.attempts, t.created_at, t.updated_at, t.run_id
            FROM scrape_tasks t
            LEFT JOIN runs r ON t.run_id = r.uuid
            WHERE t.retailer = ? AND t.status = 'pending'
            ORDER BY r.start_time DESC, 
                CASE t.status 
                    WHEN 'in_progress' THEN 0
                    WHEN 'pending' THEN 1
                    WHEN 'completed' THEN 2
                    WHEN 'failed' THEN 3 
                END,
                t.updated_at ASC
            LIMIT ?
        """, (retailer, limit))
    else:
        cur.execute("""
            SELECT t.ID, t.retailer, t.url, t.status, t.attempts, t.created_at, t.updated_at, t.run_id
            FROM scrape_tasks t
            LEFT JOIN runs r ON t.run_id = r.uuid
            WHERE t.status = 'pending'
            ORDER BY r.start_time DESC,
                CASE t.status 
                    WHEN 'in_progress' THEN 0 
                    WHEN 'pending' THEN 1 
                    WHEN 'completed' THEN 2 
                    WHEN 'failed' THEN 3 
                END,
                t.updated_at ASC
            LIMIT ?
        """, (limit,))
    
    return cur.fetchall()

def get_recent_tasks(conn, status=None, limit=20):
    cur = conn.cursor()
    
    # Status order: completed=0, in_progress=1, pending=2, failed=3
    if status:
        cur.execute("""
            SELECT t.ID, t.retailer, t.url, t.status, t.attempts, t.created_at, t.updated_at, t.run_id
            FROM scrape_tasks t
            LEFT JOIN runs r ON t.run_id = r.uuid
            WHERE t.status = ?
            ORDER BY r.start_time DESC,
                CASE t.status 
                    WHEN 'in_progress' THEN 0 
                    WHEN 'pending' THEN 1 
                    WHEN 'completed' THEN 2 
                    WHEN 'failed' THEN 3 
                END,
                t.updated_at ASC
            LIMIT ?
        """, (status, limit))
    else:
        cur.execute("""
            SELECT t.ID, t.retailer, t.url, t.status, t.attempts, t.created_at, t.updated_at, t.run_id
            FROM scrape_tasks t
            LEFT JOIN runs r ON t.run_id = r.uuid
            ORDER BY r.start_time DESC,
                CASE t.status 
                    WHEN 'in_progress' THEN 0 
                    WHEN 'pending' THEN 1 
                    WHEN 'completed' THEN 2 
                    WHEN 'failed' THEN 3 
                END,
                t.updated_at ASC
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
            
            print(f"{'Run ID':<12} | {'Task ID':<7} | {'Retailer':<8} | {'Category':<10} | {'URL':<60} | {'Att':<4}")
            print("-" * 110)
            
            for row in tasks:
                task_id, retailer, url, status, attempts, created_at, updated_at, run_id = row
                run_id_short = run_id[:9] + "..." if run_id and len(run_id) > 9 else (run_id or "")
                # Get category from runs table
                cur = conn.cursor()
                category = ""
                if run_id:
                    cur.execute("SELECT category FROM runs WHERE uuid = ?", (run_id,))
                    cat_row = cur.fetchone()
                    if cat_row:
                        category = cat_row[0] or ""
                url_display = "..." + url[-57:] if len(url) > 60 else url
                print(f"{run_id_short:<12} | {task_id:<7} | {retailer:<8} | {category:<10} | {url_display:<60} | {attempts:<4}")
                
                
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
                    SELECT t.ID, t.retailer, t.url, t.status, t.attempts, t.created_at, t.updated_at, t.run_id
                    FROM scrape_tasks t
                    LEFT JOIN runs r ON t.run_id = r.uuid
                    WHERE t.retailer = ?
                    ORDER BY r.start_time DESC,
                        CASE t.status 
                            WHEN 'in_progress' THEN 0
                            WHEN 'pending' THEN 1
                            WHEN 'completed' THEN 2
                            WHEN 'failed' THEN 3 
                        END,
                        t.updated_at ASC
                    LIMIT ?
                """, (args.retailer, args.limit))
                tasks = cur.fetchall()
            else:
                tasks = get_recent_tasks(conn, None, args.limit)
            
            if not tasks:
                print("No tasks found.")
                return
            
            print(f"{'Run ID':<12} | {'Task ID':<7} | {'Retailer':<8} | {'Category':<10} | {'Status':<12} | {'URL':<60} | {'Att':<4}")
            print("-" * 120)
            
            for row in tasks:
                task_id, retailer, url, status, attempts, created_at, updated_at, run_id = row
                run_id_short = run_id[:9] + "..." if run_id and len(run_id) > 9 else (run_id or "")
                # Get category from runs table
                cur = conn.cursor()
                category = ""
                if run_id:
                    cur.execute("SELECT category FROM runs WHERE uuid = ?", (run_id,))
                    cat_row = cur.fetchone()
                    if cat_row:
                        category = cat_row[0] or ""
                url_display = "..." + url[-57:] if len(url) > 60 else url
                print(f"{run_id_short:<12} | {task_id:<7} | {retailer:<8} | {category:<10} | {status:<12} | {url_display:<60} | {attempts:<4}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
