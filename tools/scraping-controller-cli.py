#!/usr/bin/env python3
"""
Scraping Controller CLI

A command-line tool for managing the scraping pipeline across different retailers.
Provides options to run new scraping jobs or continue from existing queue tasks.

Usage:
    python scraping-controller-cli.py --store ll --category wine --limit 10
    python scraping-controller-cli.py --store all --new
    python scraping-controller-cli.py --store bws --continue
"""
import argparse
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scraping.controller import ScrapingController
from db.databaseHandler import (
    create_connection, add_scrape_task, get_pending_tasks_count,
    get_next_pending_task
)


RETAILER_MAP = {
    'bws': 'bws',
    'll': 'll',
    'liquorland': 'll',
    'fc': 'fc',
    'firstchoice': 'fc',
    'all': 'all'
}

CATEGORY_URLS = {
    'beer': '/beer',
    'wine': '/wine',
    'spirits': '/spirits'
}

RETAILER_URLS = {
    'bws': 'https://api.bws.com.au/apis/ui/Browse?Location=%2F{category}%2Fall-{category}&banner=true&department={category}&pageNumber=1&pageSize=1000&sortType=Browse_Relevance_LocalSales&subDepartment=all+{category}',
    'll': 'https://www.liquorland.com.au{category}',
    'fc': 'https://www.firstchoiceliquor.com.au{category}'
}


def get_stores_from_arg(store_arg):
    """Convert store argument to list of stores."""
    if store_arg == 'all':
        return ['bws', 'll', 'fc']
    store_key = RETAILER_MAP.get(store_arg.lower(), store_arg.lower())
    return [store_key]


def get_categories_from_arg(category_arg):
    """Convert category argument to list of categories."""
    if category_arg is None:
        return None
    return [category_arg.lower()]


def count_pending_tasks_for_store(conn, store, category=None):
    """Count pending tasks for a store, optionally filtered by category."""
    cur = conn.cursor()
    
    if category:
        cur.execute("""
            SELECT COUNT(*) FROM scrape_tasks 
            WHERE retailer = ? AND status = 'pending' 
            AND url LIKE ?
        """, (store, f'%{category}%'))
    else:
        cur.execute("""
            SELECT COUNT(*) FROM scrape_tasks 
            WHERE retailer = ? AND status = 'pending'
        """, (store,))
    
    return cur.fetchone()[0]


def discover_tasks_for_store_category(store, category):
    """Discover tasks for a specific store and category."""
    controller = ScrapingController()
    url_template = RETAILER_URLS.get(store)
    
    if not url_template:
        print(f"Unknown store: {store}")
        return 0
    
    url = url_template.format(category=CATEGORY_URLS[category])
    
    processor = controller.processors.get(store)
    if not processor:
        print(f"No processor found for store: {store}")
        return 0
    
    conn = create_connection()
    if not conn:
        return 0
    
    try:
        tasks = processor.discover_tasks(url)
        print(f"Discovered {len(tasks)} tasks for {store}/{category}")
        
        for t in tasks:
            add_scrape_task(conn, store, t['url'], t['metadata'])
        
        return len(tasks)
    finally:
        conn.close()


def run_scraping_jobs(args):
    """Run the scraping jobs based on parsed arguments."""
    controller = ScrapingController()
    stores = get_stores_from_arg(args.store)
    
    total_completed = 0
    total_discovered = 0
    
    for store in stores:
        print(f"\n{'='*60}")
        print(f"Processing store: {store.upper()}")
        print(f"{'='*60}")
        
        if args.new:
            categories = get_categories_from_arg(args.category)
            if categories:
                for category in categories:
                    print(f"\nDiscovering tasks for {store}/{category}...")
                    discovered = discover_tasks_for_store_category(store, category)
                    total_discovered += discovered
            else:
                for category in ['beer', 'wine', 'spirits']:
                    print(f"\nDiscovering tasks for {store}/{category}...")
                    discovered = discover_tasks_for_store_category(store, category)
                    total_discovered += discovered
        
        conn = create_connection()
        if not conn:
            continue
            
        pending_count = count_pending_tasks_for_store(
            conn, store, 
            args.category if hasattr(args, 'category') and args.category else None
        )
        
        if pending_count == 0 and not args.new:
            print(f"No pending tasks for {store}. Use --new to start a fresh scrape.")
            conn.close()
            continue
        
        limit = args.limit if args.limit else None
        completed = 0
        
        print(f"\nStarting scraping for {store}...")
        print(f"Pending tasks: {pending_count}")
        if limit:
            print(f"Task limit: {limit}")
        print("-" * 40)
        
        while True:
            if limit and completed >= limit:
                print(f"\nReached task limit ({limit}). Stopping.")
                break
            
            result = controller.run_next(store)
            
            if not result:
                pending_now = count_pending_tasks_for_store(conn, store, args.category if args.category else None)
                if pending_now == 0:
                    print(f"\nNo more pending tasks for {store}.")
                else:
                    print(f"\nNo more tasks to process (limit reached or error).")
                break
            
            completed += 1
            total_completed += 1
            
            remaining = count_pending_tasks_for_store(conn, store, args.category if args.category else None)
            
            if completed % 5 == 0 or remaining == 0:
                progress_bar(completed, remaining, limit, store)
        
        print(f"\nCompleted {completed} tasks for {store}")
        conn.close()
    
    return total_completed, total_discovered


def progress_bar(completed, remaining, limit, store):
    """Display a progress bar with task information."""
    if limit:
        total = min(limit, completed + remaining)
        percentage = (completed / total) * 100 if total > 0 else 0
        bar_length = 30
        filled = int(bar_length * completed / total) if total > 0 else 0
        bar = '█' * filled + '░' * (bar_length - filled)
        
        print(f"\r[{bar}] {percentage:.1f}% | Completed: {completed} | Remaining: {remaining} | Limit: {limit} | Store: {store}", end='', flush=True)
    else:
        print(f"\rCompleted: {completed} | Remaining: {remaining} | Store: {store}", end='', flush=True)
    
    print()


def main():
    parser = argparse.ArgumentParser(
        description='Scraping Controller CLI - Manage scraping jobs across retailers',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --store ll --category wine --limit 10
  %(prog)s --store bws --new
  %(prog)s --store all --category beer --limit 50
  %(prog)s --store ll --continue
  
Store options: bws, ll (liquorland), fc (first choice), all
Category options: beer, wine, spirits
        """
    )
    
    parser.add_argument(
        '-s', '--store',
        type=str,
        default='ll',
        help='Store to scrape: bws, ll, fc, all (default: ll)'
    )
    
    parser.add_argument(
        '-c', '--category',
        type=str,
        default=None,
        choices=['beer', 'wine', 'spirits'],
        help='Category to scrape: beer, wine, spirits (default: all)'
    )
    
    parser.add_argument(
        '-l', '--limit',
        type=int,
        default=None,
        help='Maximum number of tasks to process (default: unlimited)'
    )
    
    parser.add_argument(
        '-n', '--new',
        action='store_true',
        help='Start a new scraping run (discovers new tasks)'
    )
    
    parser.add_argument(
        '--continue',
        dest='continue_run',
        action='store_true',
        help='Continue from existing queue tasks (default behavior)'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        print(f"Store: {args.store}")
        print(f"Category: {args.category}")
        print(f"Limit: {args.limit}")
        print(f"New: {args.new}")
        print(f"Continue: {args.continue_run}")
        print()
    
    conn = create_connection()
    if not conn:
        print("Error: Could not connect to database")
        sys.exit(1)
    
    if not args.new and args.continue_run:
        stores = get_stores_from_arg(args.store)
        has_tasks = False
        
        for store in stores:
            if args.category:
                count = count_pending_tasks_for_store(conn, store, args.category)
            else:
                count = get_pending_tasks_count(conn, store)
            
            if count > 0:
                has_tasks = True
                break
        
        if not has_tasks:
            print("\nNo in-progress run detected.")
            print("There are no pending tasks in the queue for the specified criteria.")
            print("To start a new scraping run, use: --new or -n")
            conn.close()
            sys.exit(0)
    
    conn.close()
    
    if not args.new and not args.continue_run:
        args.continue_run = True
    
    completed, discovered = run_scraping_jobs(args)
    
    print(f"\n{'='*60}")
    print(f"Scraping run complete!")
    print(f"Tasks discovered: {discovered}")
    print(f"Tasks completed: {completed}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
