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
import uuid
from pathlib import Path
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn

console = Console()

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
    'all': 'all'
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


def count_pending_tasks_by_run(conn, run_id, category=None):
    """Count pending tasks for a specific run, optionally filtered by category."""
    cur = conn.cursor()
    
    if category:
        cur.execute("""
            SELECT COUNT(*) FROM scrape_tasks 
            WHERE run_id = ? AND status = 'pending' 
            AND url LIKE ?
        """, (run_id, f'%{category}%'))
    else:
        cur.execute("""
            SELECT COUNT(*) FROM scrape_tasks 
            WHERE run_id = ? AND status = 'pending'
        """, (run_id,))
    
    return cur.fetchone()[0]


def discover_tasks_for_store_category(store, category=None, run_id=None):
    """Discover tasks for a specific store and category. Returns (count, run_id)."""
    controller = ScrapingController()
    
    # Call discover with category - it handles URL building internally
    discovered_run_id = controller.discover(store, category=category, run_id=run_id)
    
    # Count how many tasks were added
    conn = create_connection()
    cur = conn.cursor()
    if run_id:
        cur.execute("SELECT COUNT(*) FROM scrape_tasks WHERE run_id = ?", (run_id,))
    else:
        cur.execute("SELECT COUNT(*) FROM scrape_tasks WHERE run_id = ?", (discovered_run_id,))
    count = cur.fetchone()[0]
    conn.close()
    
    return count, discovered_run_id


def run_scraping_jobs(args):
    """Run the scraping jobs based on parsed arguments."""
    controller = ScrapingController()
    stores = get_stores_from_arg(args.store)
    
    total_completed = 0
    total_discovered = 0
    current_run_id = None
    
    for store in stores:
        print(f"\n{'='*60}")
        print(f"Processing store: {store.upper()}")
        print(f"{'='*60}")
        
        if args.new:
            categories = get_categories_from_arg(args.category)
            if categories:
                for category in categories:
                    print(f"\nDiscovering tasks for {store}/{category}...")
                    discovered, current_run_id = discover_tasks_for_store_category(store, category)
                    total_discovered += discovered
            else:
                for category in ['beer', 'wine', 'spirits', 'premix']:
                    print(f"\nDiscovering tasks for {store}/{category}...")
                    discovered, current_run_id = discover_tasks_for_store_category(store, category)
                    total_discovered += discovered
        else:
            # When continuing (not --new), get the most recent run for this store/category
            conn = create_connection()
            cur = conn.cursor()
            if args.category:
                cur.execute("""
                    SELECT uuid FROM runs 
                    WHERE retailer = ? AND category = ? 
                    ORDER BY start_time DESC LIMIT 1
                """, (store, args.category))
            else:
                cur.execute("""
                    SELECT uuid FROM runs 
                    WHERE retailer = ? 
                    ORDER BY start_time DESC LIMIT 1
                """, (store,))
            row = cur.fetchone()
            if row:
                current_run_id = row[0]
            conn.close()
            conn = create_connection()
            
            if not conn:
                continue
        
        # Get pending count - use run_id if available
        if current_run_id:
            pending_count = count_pending_tasks_by_run(
                conn, current_run_id,
                args.category if hasattr(args, 'category') and args.category else None
            )
        else:
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
        if current_run_id:
            print(f"Run ID: {current_run_id[:9]}...")
        if limit:
            console.print(f"[bold]Task limit:[/bold] {limit}")
        console.print("-" * 40)
        
        # Create progress bar
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("[cyan]{task.fields[remaining]} remaining"),
            console=console
        ) as progress:
            
            if limit:
                task = progress.add_task(
                    f"[green]Scraping {store}", 
                    total=limit, 
                    remaining=pending_count
                )
            else:
                task = progress.add_task(
                    f"[green]Scraping {store}", 
                    total=pending_count, 
                    remaining=pending_count
                )
            
            while True:
                if limit and completed >= limit:
                    progress.update(task, description=f"[yellow]Limit reached")
                    break
                
                result = controller.run_next(store, run_id=current_run_id)
                
                if not result:
                    if current_run_id:
                        pending_now = count_pending_tasks_by_run(conn, current_run_id, args.category if args.category else None)
                    else:
                        pending_now = count_pending_tasks_for_store(conn, store, args.category if args.category else None)
                    if pending_now == 0:
                        progress.update(task, description=f"[green]Completed - no more tasks")
                    else:
                        progress.update(task, description=f"[red]Stopped - error or limit")
                    break
                
                completed += 1
                total_completed += 1
                
                if current_run_id:
                    remaining = count_pending_tasks_by_run(conn, current_run_id, args.category if args.category else None)
                else:
                    remaining = count_pending_tasks_for_store(conn, store, args.category if args.category else None)
                
                if limit:
                    progress.update(task, completed=completed, remaining=remaining)
                else:
                    progress.update(task, completed=completed, total=completed + remaining, remaining=remaining)
        
        console.print(f"\n[bold green]Completed {completed} tasks for {store}[/bold green]")
        conn.close()
    
    return total_completed, total_discovered


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
Category options: beer, wine, spirits, premix
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
        choices=['beer', 'wine', 'spirits', 'premix'],
        help='Category to scrape: beer, wine, spirits, premix (default: all)'
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
        '--resume-last',
        action='store_true',
        help='Resume the last run for the specified retailer/category'
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
        print(f"Resume Last: {args.resume_last}")
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
    
    # Handle resume-last - find the last run and set args to continue it
    if args.resume_last:
        args.new = False
        args.continue_run = True
        # Verify the run exists
        conn = create_connection()
        cur = conn.cursor()
        
        stores = get_stores_from_arg(args.store)
        
        for store in stores:
            if args.category:
                cur.execute("""
                    SELECT uuid FROM runs 
                    WHERE retailer = ? AND category = ? 
                    ORDER BY start_time DESC LIMIT 1
                """, (store, args.category))
            else:
                cur.execute("""
                    SELECT uuid FROM runs 
                    WHERE retailer = ? 
                    ORDER BY start_time DESC LIMIT 1
                """, (store,))
            
            row = cur.fetchone()
            if row:
                break
        
        conn.close()
        
        if not row:
            print("\nNo previous run found for the specified criteria.")
            print("To start a new scraping run, use: --new or -n")
            sys.exit(0)
    
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
