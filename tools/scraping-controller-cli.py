#!/usr/bin/env python3
"""
Scraping Controller CLI

A command-line tool for managing the scraping pipeline across different retailers.
Provides options to run new scraping jobs or continue from existing queue tasks.

Usage:
    python scraping-controller-cli.py --store=ll --category=wine --limit=10
    python scraping-controller-cli.py --store=all --new
    python scraping-controller-cli.py --store=bws --continue
"""
import argparse
import sys
import os
import uuid
import queue
from pathlib import Path
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

console = Console()

sys.path.insert(0, str(Path(__file__).parent.parent))

from scraping.controller import ScrapingController
from db.databaseHandler import (
    create_connection, get_pending_tasks_count
)

MAN_PAGE = """SCRAPING CONTROLLER CLI(1)     CompareTheBrew     SCRAPING CONTROLLER CLI(1)

NAME
       scraping-controller-cli - Task-based scraping controller CLI for CompareTheBrew

SYNOPSIS
       python scraping-controller-cli.py [OPTIONS]

DESCRIPTION
       Coordinates the scraping pipeline across different retailers using
       a Task Queue system and exposes progress through Rich-based CLI output.

ARGUMENTS
       --store=STORE
              The retailer to scrape. Supported values: bws, ll, fc, all

       --discover
              Seeds the task queue with URLs to scrape.

       --new
              Start a new scraping run (discovers new tasks) before processing.

       --continue
              Continue from the existing queue tasks (default behavior when not --new).

       --resume-last
              Resume the most recent run for the specified retailer/category.

       --category=CAT
              Filter discovery by category (beer, wine, spirits, premix).

       --limit=N
              Limit the number of tasks to process in this run.

       --workers=N
              Number of worker threads to use (default: 1). Use >1 for parallelism.

       --man
              Display this detailed man page.

       -h, --help
              Show brief help text provided by argparse.

EXAMPLES
       python scraping-controller-cli.py --store=ll --category=wine --limit=10 --workers=2
       python scraping-controller-cli.py --store=bws --new --limit=3
       python scraping-controller-cli.py --store=fc --workers=4 --category=spirits
       python scraping-controller-cli.py --store=ll --continue --limit=2
       python scraping-controller-cli.py --man

AUTHORS
       CompareTheBrew Team
"""


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


def count_pending_tasks_for_store(conn, store, category=None, task_type='page'):
    """Count pending tasks for a store, optionally filtered by category."""
    cur = conn.cursor()
    
    if category:
        query = """
            SELECT COUNT(*) FROM scrape_tasks 
            WHERE retailer = ? AND status = 'pending' 
            AND url LIKE ?
        """
        params = [store, f'%{category}%']
    else:
        query = """
            SELECT COUNT(*) FROM scrape_tasks 
            WHERE retailer = ? AND status = 'pending'
        """
        params = [store]
    
    if task_type:
        query += " AND task_type = ?"
        params.append(task_type)
    
    cur.execute(query, params)
    return cur.fetchone()[0]

def count_pending_tasks_by_run(conn, run_id, category=None, task_type='page'):
    """Count pending tasks for a specific run, optionally filtered by category."""
    cur = conn.cursor()
    
    if category:
        query = """
            SELECT COUNT(*) FROM scrape_tasks 
            WHERE run_id = ? AND status = 'pending' 
            AND url LIKE ?
        """
        params = [run_id, f'%{category}%']
    else:
        query = """
            SELECT COUNT(*) FROM scrape_tasks 
            WHERE run_id = ? AND status = 'pending'
        """
        params = [run_id]

    if task_type:
        query += " AND task_type = ?"
        params.append(task_type)

    cur.execute(query, params)
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


def monitor_run_progress(event_queue: queue.Queue, store: str, limit: int | None):
    drinks_expected = 0
    drinks_processed = 0
    page_completed = 0
    detail_completed = 0
    page_total_target = limit or 1
    pending = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console
    ) as progress:
        page_task = progress.add_task(
            description=f"[green]{store} Pages | Pending 0",
            total=page_total_target
        )
        drink_task = progress.add_task(
            description=f"[cyan]{store} Drinks | Awaiting drinks...",
            total=0
        )

        while True:
            event = event_queue.get()
            event_type = event.get("type")
            console.print(f"[dim]Received event {event_type}: {event}")

            if event_type == "run_started":
                pending = event.get("pending", 0)
                page_total_target = limit or max(pending, 1)
                desc = f"[green]{store}: Tasks 0/{page_total_target} | Pending {pending}"
                progress.update(page_task, total=max(page_total_target, 1), completed=0, description=desc)

            elif event_type == "page_items":
                drinks_expected += event.get("count", 0)
                progress.update(drink_task, total=max(drinks_expected, drinks_processed))

            elif event_type == "drink_processed":
                drinks_processed = event.get("drinks_processed", drinks_processed + 1)
                action = "inserted" if event.get("inserted") else "updated"
                brand = event.get("brand") or ""
                name = event.get("name") or ""
                drink_label = f"{brand} {name}".strip()
                progress.update(drink_task, completed=drinks_processed, description=f"[cyan]{drink_label}")
                absolute_index = event.get("absolute_index", drinks_processed)
                page_total = event.get("page_total")
                if not page_total:
                    page_total = drinks_expected or drinks_processed
                console.print(
                    f"[dim]Drink {absolute_index}/{page_total} ({action}):[/dim] "
                    f"{drink_label} | "
                    f"ABV {event.get('percent', 0.0):.2f}% | "
                    f"StdDrinks {event.get('std_drinks', 0.0):.2f} | "
                    f"Price ${event.get('price', 0.0):.2f}"
                )

            elif event_type == "task_completed":
                page_completed = event.get("page_completed", page_completed)
                detail_completed = event.get("detail_completed", detail_completed)
                pending = event.get("pending", pending)
                total_goal = limit or max(page_completed + pending, 1)
                desc = f"[green]{store}: Tasks {page_completed}/{total_goal} | Pending {pending}"
                if limit:
                    desc += f" (limit {limit})"
                if detail_completed:
                    desc += f" | Detail updates: {detail_completed}"
                progress.update(
                    page_task,
                    completed=page_completed,
                    total=max(total_goal, page_total_target),
                    description=desc
                )
                page_total_target = max(page_total_target, total_goal)

            elif event_type == "run_completed":
                pending = event.get("pending", pending)
                summary = event
                progress.update(page_task, completed=summary.get("pages", page_completed))
                break

    return summary

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
            if not conn:
                continue
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
        num_workers = args.workers if getattr(args, "workers", None) and args.workers > 0 else 1

        print(f"\nStarting scraping for {store}...")
        print(f"Pending tasks: {pending_count}")
        if current_run_id:
            print(f"Run ID: {current_run_id[:9]}...")
        if limit:
            console.print(f"[bold]Task limit:[/bold] {limit}")
        console.print("-" * 40)

        event_queue = controller.start_run(store, limit=limit, run_id=current_run_id, num_workers=num_workers)
        summary = monitor_run_progress(event_queue, store, limit)
        if summary:
            total_completed += summary.get("pages", 0)
            console.print(f"\n[bold green]Completed {summary.get('pages', 0)} page tasks "
                          f"(detail updates: {summary.get('details', 0)}) for {store}[/bold green]")
        conn.close()
    
    return total_completed, total_discovered


def main():
    parser = argparse.ArgumentParser(
        description='Scraping Controller CLI - Manage scraping jobs across retailers',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --store=ll --category=wine --limit=10
  %(prog)s --store=bws --new
  %(prog)s --store=all --category=beer --limit=50
  %(prog)s --store=ll --continue
  
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
        '--workers',
        type=int,
        default=1,
        help='Number of worker threads to use (default: 1)'
    )

    parser.add_argument(
        '--man',
        action='store_true',
        help='Display the detailed manual for this CLI'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    if args.man:
        print(MAN_PAGE)
        sys.exit(0)

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
                count = count_pending_tasks_for_store(conn, store, args.category, task_type=None)
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
