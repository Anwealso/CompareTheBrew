import argparse
import sqlite3
import sys
from pathlib import Path
from datetime import datetime

from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.reactive import reactive
from textual.widgets import Header, Footer, Static, DataTable


def get_db_path():
    return Path(__file__).parent.parent / "db" / "database.db"


def create_connection():
    db_path = get_db_path()
    if not db_path.exists():
        return None
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error:
        return None


def get_runs_with_tasks(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT r.uuid, r.retailer, r.category, r.start_time, r.status,
               (SELECT COUNT(*) FROM scrape_tasks t WHERE t.run_id = r.uuid) as total,
               (SELECT COUNT(*) FROM scrape_tasks t WHERE t.run_id = r.uuid AND t.status = 'pending') as pending,
               (SELECT COUNT(*) FROM scrape_tasks t WHERE t.run_id = r.uuid AND t.status = 'in_progress') as in_progress,
               (SELECT COUNT(*) FROM scrape_tasks t WHERE t.run_id = r.uuid AND t.status = 'completed') as completed,
               (SELECT COUNT(*) FROM scrape_tasks t WHERE t.run_id = r.uuid AND t.status = 'failed') as failed
        FROM runs r
        ORDER BY r.start_time DESC
    """)
    return cur.fetchall()


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


def get_tasks(conn, retailer=None, status=None, limit=1000):
    cur = conn.cursor()
    query = """
        SELECT t.ID, t.retailer, t.url, t.status, t.attempts, t.created_at, t.updated_at, t.run_id
        FROM scrape_tasks t
        LEFT JOIN runs r ON t.run_id = r.uuid
        WHERE 1=1
    """
    params = []
    if retailer:
        query += " AND t.retailer = ?"
        params.append(retailer)
    if status:
        query += " AND t.status = ?"
        params.append(status)
    query += """
        ORDER BY r.start_time DESC,
            CASE t.status 
                WHEN 'in_progress' THEN 0 
                WHEN 'pending' THEN 1 
                WHEN 'completed' THEN 2 
                WHEN 'failed' THEN 3 
            END,
            t.updated_at ASC
        LIMIT ?
    """
    params.append(limit)
    cur.execute(query, params)
    return cur.fetchall()


def get_category_for_run(conn, run_id):
    if not run_id:
        return ""
    cur = conn.cursor()
    cur.execute("SELECT category FROM runs WHERE uuid = ?", (run_id,))
    row = cur.fetchone()
    return row[0] if row and row[0] else ""


def get_tasks_for_run(conn, run_id, limit=50000):
    if not run_id:
        return []
    cur = conn.cursor()
    cur.execute("""
        SELECT t.ID, t.retailer, t.url, t.status, t.attempts, t.created_at, t.updated_at, t.run_id
        FROM scrape_tasks t
        WHERE t.run_id LIKE ?
        ORDER BY 
            CASE t.status 
                WHEN 'completed' THEN 0 
                WHEN 'in_progress' THEN 1 
                WHEN 'pending' THEN 2 
                WHEN 'failed' THEN 3 
            END,
            t.updated_at ASC
        LIMIT ?
    """, (run_id + "%", limit))
    return cur.fetchall()


class TaskQueueTable(DataTable):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cursor_type = "row"

    def on_data_table_row_selected(self, event) -> None:
        self.app.action_view_details()


class TaskQueueApp(App):
    CSS = """
    Screen {
        background: $surface;
    }
    
    #container {
        height: 90%;
        padding: 0 1;
    }
    
    #stats {
        height: auto;
        padding: 0 1;
        background: $panel;
        border-bottom: solid $primary;
    }
    
    #stats Static {
        margin: 0;
    }
    
    TaskQueueTable {
        height: 100%;
        border: none;
    }
    
    TaskQueueTable:focus {
        border: solid $primary;
    }
    
    TaskQueueTable > .datatable--cursor {
        background: $accent;
    }
    
    #footer-bar {
        height: auto;
        padding: 0 1;
        background: $panel;
        border-top: solid $primary;
    }
    
    #footer-bar Static {
        opacity: 0.8;
        margin: 0;
    }
    """

    BINDINGS = [
        ("q,escape", "quit", "Quit"),
        ("r", "refresh", "Refresh"),
        ("up,w", "cursor_up", "Up"),
        ("down,s", "cursor_down", "Down"),
        ("home", "cursor_home", "Home"),
        ("end", "cursor_end", "End"),
        ("enter", "view_details", "View Details"),
        ("backspace", "go_back", "Back"),
    ]

    retailer_filter = reactive("")
    status_filter = reactive("")
    view_mode = reactive("overview")
    selected_run_id = reactive("")
    selected_retailer = reactive("")
    selected_category = reactive("")

    def __init__(self, retailer: str = None, status: str = None, **kwargs):
        super().__init__(**kwargs)
        self.retailer_filter = retailer or ""
        self.status_filter = status or ""
        self.conn = create_connection()

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(id="container"):
            with Vertical(id="stats"):
                yield Static("Loading...", id="stats-text")
            yield TaskQueueTable(id="task-table")
        with Horizontal(id="footer-bar"):
            yield Static("", id="footer-text")

    def on_mount(self) -> None:
        table = self.query_one("#task-table", TaskQueueTable)
        table.add_columns("Run ID", "Retailer", "Category", "Total", "Pending", "In Progress", "Completed", "Failed")
        table.focus()
        self.query_one("#footer-text", Static).update("[b]Enter[/b]: Select/View | [b]↑/↓[/b]: Navigate | [b]Backspace[/b]: Go Back | [b]Q[/b]: Quit")
        self.refresh_data()
        self.set_interval(2.0, self.refresh_data)

    def action_refresh(self) -> None:
        self.refresh_data()

    def action_cursor_up(self) -> None:
        table = self.query_one("#task-table", TaskQueueTable)
        table.action_cursor_up()

    def action_cursor_down(self) -> None:
        table = self.query_one("#task-table", TaskQueueTable)
        table.action_cursor_down()

    def action_cursor_home(self) -> None:
        table = self.query_one("#task-table", TaskQueueTable)
        table.action_cursor_home()

    def action_cursor_end(self) -> None:
        table = self.query_one("#task-table", TaskQueueTable)
        table.action_cursor_end()

    def action_view_details(self) -> None:
        table = self.query_one("#task-table", TaskQueueTable)
        cursor_row = table.cursor_row
        runs = get_runs_with_tasks(self.conn)
        
        if cursor_row is not None and cursor_row < len(runs):
            run = runs[cursor_row]
            self.selected_run_id = run[0] or ""
            self.selected_retailer = run[1] or ""
            self.selected_category = run[2] or ""
            self.view_mode = "details"
            self.refresh_data()

    def action_go_back(self) -> None:
        if self.view_mode == "details":
            self.view_mode = "overview"
            self.selected_run_id = ""
            self.selected_retailer = ""
            self.selected_category = ""
            self.refresh_data()

    def refresh_data(self) -> None:
        if not self.conn:
            self.conn = create_connection()
        if not self.conn:
            self.query_one("#stats-text", Static).update("Error: Could not connect to database")
            return

        try:
            table = self.query_one("#task-table", TaskQueueTable)
            cursor_row = table.cursor_row
            cursor_column = table.cursor_column
            
            table.clear()
            for col in list(table.columns.keys()):
                table.remove_column(col)

            if self.view_mode == "details" and self.selected_run_id:
                tasks = get_tasks_for_run(self.conn, self.selected_run_id)
                
                pending = sum(1 for t in tasks if t[3] == 'pending')
                in_progress = sum(1 for t in tasks if t[3] == 'in_progress')
                completed = sum(1 for t in tasks if t[3] == 'completed')
                failed = sum(1 for t in tasks if t[3] == 'failed')
                
                header_text = f"[DETAILED VIEW] Run: {self.selected_run_id} | {self.selected_retailer} | {self.selected_category} | T:{len(tasks)} P:{pending} IP:{in_progress} C:{completed} F:{failed}"
                self.query_one("#stats-text", Static).update(header_text)
                
                table.add_columns("Task ID", "Retailer", "Category", "Status", "URL", "Attempts")
                
                for row in tasks:
                    task_id, retailer, url, status, attempts, created_at, updated_at, run_id = row
                    category = self.selected_category
                    url_display = url[:80] + "..." if len(url) > 83 else url
                    table.add_row(
                        str(task_id),
                        retailer,
                        category,
                        status,
                        url_display,
                        str(attempts)
                    )
                
                if tasks:
                    max_row = len(tasks) - 1
                    restore_row = min(cursor_row, max_row)
                    table.move_cursor(row=restore_row, column=cursor_column)
                
                self.query_one("#footer-text", Static).update("[b]Backspace[/b]: Back to Overview | [b]↑/↓[/b]: Navigate | [b]Q[/b]: Quit")
            else:
                runs = get_runs_with_tasks(self.conn)
                stats = get_task_stats(self.conn, self.retailer_filter or None)
                
                stats_text = f"Last updated: {datetime.now().strftime('%H:%M:%S')} | "
                total_pending = total_in_progress = total_completed = total_failed = 0
                for status, count in stats:
                    if status == 'pending':
                        total_pending = count
                    elif status == 'in_progress':
                        total_in_progress = count
                    elif status == 'completed':
                        total_completed = count
                    elif status == 'failed':
                        total_failed = count
                stats_text += f"Total: Pending={total_pending} | In Progress={total_in_progress} | Completed={total_completed} | Failed={total_failed}"
                
                if self.retailer_filter:
                    stats_text += f" | Filter: retailer={self.retailer_filter}"
                if self.status_filter:
                    stats_text += f" | Filter: status={self.status_filter}"
                
                self.query_one("#stats-text", Static).update(stats_text)
                
                table.add_columns("Run ID", "Retailer", "Category", "Total", "Pending", "In Progress", "Completed", "Failed")
                
                for row in runs:
                    run_id, retailer, category, start_time, run_status, total, pending, in_progress, completed_count, failed = row
                    run_id_short = run_id[:10] + ".." if run_id and len(run_id) > 12 else (run_id or "")
                    category_short = (category or "")[:15]
                    table.add_row(
                        run_id_short,
                        retailer,
                        category_short,
                        str(total),
                        str(pending),
                        str(in_progress),
                        str(completed_count),
                        str(failed)
                    )
                
                if runs:
                    max_row = len(runs) - 1
                    restore_row = min(cursor_row, max_row)
                    table.move_cursor(row=restore_row, column=cursor_column)
                
                self.query_one("#footer-text", Static).update("[b]Enter[/b]: Select/View | [b]↑/↓[/b]: Navigate | [b]Q[/b]: Quit")

        except Exception as e:
            self.query_one("#stats-text", Static).update(f"Error: {e}")
        finally:
            pass

    def on_unmount(self) -> None:
        self.conn = None


def main():
    parser = argparse.ArgumentParser(description='CompareTheBrew Task Queue CLI')
    parser.add_argument('--retailer', type=str, help='Filter by retailer (e.g., bws, danmurphys)')
    parser.add_argument('--status', type=str, choices=['pending', 'in_progress', 'completed', 'failed'], 
                        help='Filter by status')
    parser.add_argument('--limit', type=int, default=20, help='Number of tasks to show (default: 20)')
    parser.add_argument('--show-pending', action='store_true', help='Show pending tasks')
    parser.add_argument('--show-stats', action='store_true', help='Show task statistics')
    parser.add_argument('--watch', action='store_true', help='Live update mode (auto-refresh)')
    parser.add_argument('--refresh-interval', type=float, default=2.0, help='Refresh interval in seconds for --watch mode')
    
    args = parser.parse_args()

    conn = create_connection()
    if not conn:
        print("Error: Could not connect to database")
        sys.exit(1)

    if args.watch:
        app = TaskQueueApp(retailer=args.retailer, status=args.status)
        app.run()
        return

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
            tasks = get_tasks(conn, args.retailer, 'pending', args.limit)
            
            if not tasks:
                print("No pending tasks found.")
                return
            
            print(f"{'Run ID':<12} | {'Task ID':<7} | {'Retailer':<8} | {'Category':<10} | {'URL':<60} | {'Att':<4}")
            print("-" * 110)
            
            for row in tasks:
                task_id, retailer, url, status, attempts, created_at, updated_at, run_id = row
                run_id_short = run_id[:9] + "..." if run_id and len(run_id) > 9 else (run_id or "")
                category = get_category_for_run(conn, run_id)
                url_display = "..." + url[-57:] if len(url) > 60 else url
                print(f"{run_id_short:<12} | {task_id:<7} | {retailer:<8} | {category:<10} | {url_display:<60} | {attempts:<4}")
                
        else:
            print(f"\n=== Recent Tasks ===")
            if args.status:
                print(f" (filtered by status: {args.status})")
            elif args.retailer:
                print(f" (filtered by retailer: {args.retailer})")
            print()
            
            tasks = get_tasks(conn, args.retailer, args.status, args.limit)
            
            if not tasks:
                print("No tasks found.")
                return
            
            print(f"{'Run ID':<12} | {'Task ID':<7} | {'Retailer':<8} | {'Category':<10} | {'Status':<12} | {'URL':<60} | {'Att':<4}")
            print("-" * 120)
            
            for row in tasks:
                task_id, retailer, url, status, attempts, created_at, updated_at, run_id = row
                run_id_short = run_id[:9] + "..." if run_id and len(run_id) > 9 else (run_id or "")
                category = get_category_for_run(conn, run_id)
                url_display = "..." + url[-57:] if len(url) > 60 else url
                print(f"{run_id_short:<12} | {task_id:<7} | {retailer:<8} | {category:<10} | {status:<12} | {url_display:<60} | {attempts:<4}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
