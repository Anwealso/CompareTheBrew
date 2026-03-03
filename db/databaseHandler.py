import sqlite3
from sqlite3 import Error
from pathlib import Path
from scripts.classItem import Item
from search.intellisearch import build_search_text, intellisearch
import itertools
import operator
import json


def get_schema_dir():

    """Get the path to the schema directory."""
    return Path(__file__).parent / "schema" / "tables"


def ensure_tables(conn):
    """Create tables from schema files if they don't exist."""
    schema_dir = get_schema_dir()
    cur = conn.cursor()
    
    for schema_file in sorted(schema_dir.glob("*.sql")):
        if schema_file.stem == "schema_version":
            continue
        sql = schema_file.read_text()
        cur.executescript(sql)
    
    conn.commit()


def create_connection():
    conn = None
    try:
        conn = sqlite3.connect(str(Path(__file__).parent / "database.db"))
        ensure_tables(conn)
        print("connected to database")
    except Error as e:
        print(e)

    return conn


def create_metrics_connection():
    conn = None
    try:
        conn = sqlite3.connect(str(Path(__file__).parent / "database.db"))
        ensure_tables(conn)
        print("connected to metrics")
    except Error as e:
        print(e)

    return conn


def create_entry(conn, task):
    """
    Create a new task
    :param conn:
    :param task:
    :return:
    """

    sql = ''' INSERT INTO drinks(store,brand,name,type,price,link,ml,percent,stdDrinks,efficiency,image,search_text)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, task)
    return cur.lastrowid



def upsert_source(conn, url, retailer, last_scraped):
    """
    Update or insert a source
    :param conn:
    :param url:
    :param retailer:
    :param last_scraped:
    :return:
    """
    sql = ''' INSERT INTO sources(url, retailer, last_scraped)
              VALUES(?,?,?)
              ON CONFLICT(url) DO UPDATE SET last_scraped=excluded.last_scraped '''
    cur = conn.cursor()
    cur.execute(sql, (url, retailer, last_scraped))
    conn.commit()
    return cur.lastrowid


def get_sources_by_retailer(conn, retailer):
    """
    Get all sources for a retailer
    :param conn:
    :param retailer:
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM sources WHERE retailer=?", (retailer,))
    return cur.fetchall()


def add_scrape_task(conn, retailer, url, metadata=None):
    """
    Add a new scrape task to the queue
    """
    from datetime import datetime
    now = datetime.now().isoformat()
    sql = ''' INSERT INTO scrape_tasks(retailer, url, status, metadata, created_at, updated_at)
              VALUES(?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, (retailer, url, 'pending', json.dumps(metadata) if metadata else None, now, now))
    conn.commit()
    return cur.lastrowid


def get_next_pending_task(conn, retailer=None):
    """
    Get the next pending task, optionally filtered by retailer.
    Sorted by updated_at (asc) to ensure failed/re-queued tasks 
    (with fresh updated_at) move to the back.
    """
    cur = conn.cursor()
    if retailer:
        cur.execute("SELECT * FROM scrape_tasks WHERE retailer=? AND status='pending' ORDER BY updated_at ASC LIMIT 1", (retailer,))
    else:
        cur.execute("SELECT * FROM scrape_tasks WHERE status='pending' ORDER BY updated_at ASC LIMIT 1")
    return cur.fetchone()


def update_task_status(conn, task_id, status, metadata=None):
    """
    Update the status and metadata of a task.
    If status is 'pending' (retry), update updated_at to move it to the back of the queue.
    """
    from datetime import datetime
    now = datetime.now().isoformat()
    
    cur = conn.cursor()
    if status == 'pending':
        # Move to back of the queue by setting updated_at to NOW.
        if metadata:
            sql = ''' UPDATE scrape_tasks SET status=?, metadata=?, updated_at=? WHERE ID=? '''
            cur.execute(sql, (status, json.dumps(metadata), now, task_id))
        else:
            sql = ''' UPDATE scrape_tasks SET status=?, updated_at=? WHERE ID=? '''
            cur.execute(sql, (status, now, task_id))
    else:
        if metadata:
            sql = ''' UPDATE scrape_tasks SET status=?, metadata=?, updated_at=? WHERE ID=? '''
            cur.execute(sql, (status, json.dumps(metadata), now, task_id))
        else:
            sql = ''' UPDATE scrape_tasks SET status=?, updated_at=? WHERE ID=? '''
            cur.execute(sql, (status, now, task_id))
    conn.commit()




def increment_task_attempts(conn, task_id):
    """
    Increment the attempts count for a task
    """
    sql = ''' UPDATE scrape_tasks SET attempts = attempts + 1 WHERE ID = ? '''
    cur = conn.cursor()
    cur.execute(sql, (task_id,))
    conn.commit()


def get_pending_tasks_count(conn, retailer=None):
    """
    Get count of pending tasks
    """
    cur = conn.cursor()
    if retailer:
        cur.execute("SELECT COUNT(*) FROM scrape_tasks WHERE retailer=? AND status='pending'", (retailer,))
    else:
        cur.execute("SELECT COUNT(*) FROM scrape_tasks WHERE status='pending'")
    return cur.fetchone()[0]


def create_metric_entry(conn, task):
    """
    Create a new task
    :param conn:
    :param task:
    :return:
    """
    print(1)
    sql = ''' INSERT INTO metrics(IP,query,datetime,country,region,city,lat,long,hostname,org)
              VALUES(?,?,?,?,?,?,?,?,?,?) '''
    print(2)
    cur = conn.cursor()
    print(3)
    cur.execute(sql, task)
    print(4)
    conn.commit()
    ID = cur.lastrowid
    conn.close()
    return ID


def select_all_drinks(conn):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM drinks")

    rows = cur.fetchall()

    return rows


def select_all_drinks_by_efficiency(conn):
    """
    Query tasks by efficiency
    :param conn: the Connection object
    :return:
    """
    # Create a new cursor
    cur = conn.cursor()
    # Ececute a new query at the cursor
    cur.execute("SELECT * FROM drinks ORDER BY efficiency DESC")
    # Fetch all of the rows that matched the query
    rows = cur.fetchall()
    return rows


def select_all_drinks_by_worst_efficiency(conn):
    """
    Query tasks by efficiency
    :param conn: the Connection object
    :return:
    """
    # Create a new cursor
    cur = conn.cursor()
    # Ececute a new query at the cursor
    cur.execute("SELECT * FROM drinks ORDER BY efficiency ASC")
    # Fetch all of the rows that matched the query
    rows = cur.fetchall()
    return rows


def select_all_drinks_by_cost_asc(conn):
    """
    Query tasks by price ascending
    :param conn: the Connection object
    :return:
    """
    # Create a new cursor
    cur = conn.cursor()
    # Ececute a new query at the cursor
    cur.execute("SELECT * FROM drinks ORDER BY price ASC")
    # Fetch all of the rows that matched the query
    rows = cur.fetchall()

    return rows


def select_all_drinks_by_cost_desc(conn):
    """
    Query tasks by price descending
    :param conn: the Connection object
    :return:
    """
    # Create a new cursor
    cur = conn.cursor()
    # Ececute a new query at the cursor
    cur.execute("SELECT * FROM drinks ORDER BY price DESC")
    # Fetch all of the rows that matched the query
    rows = cur.fetchall()

    return rows


def select_all_drinks_between_cost(conn, value1, value2):
    """
    Query tasks by selecting drinks between a cost
    :param conn: the Connection object
    :return:
    """
    # Create a new cursor
    cur = conn.cursor()

    # Ececute a new query at the cursor
    cur.execute("SELECT * FROM drinks WHERE price BETWEEN {} AND {} ORDER BY efficiency DESC".format(value1, value2))
    # Fetch all of the rows that matched the query
    rows = cur.fetchall()

    return rows


def select_drink_by_efficiency_and_type(conn, type):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param type: the value in the type column that we are querying for
    :param priority:
    :return:
    """
    # Create a new cursor
    cur = conn.cursor()
    # Execute a new query at the cursor
    cur.execute("SELECT * FROM drinks WHERE type LIKE '%{}%' ORDER BY efficiency DESC".format(type))
    # Fetch all of the rows that matched the query
    rows = cur.fetchall()

    return rows


def select_image_links(conn):
    """
    Query all image links
    :param conn: the Connection object
    :return:
    """
    # Create a new cursor
    cur = conn.cursor()
    # Execute a new query at the cursor
    cur.execute("SELECT image FROM drinks")
    # Fetch all of the rows that matched the query
    rows = cur.fetchall()

    return rows


def select_drink_by_smart_search(conn, terms, thing, price_min="", price_max=""):
    """Select all drinks that contain any of the search keywords given in their name, brand or type attributes
    
    Args:
        conn: the Connection object
        terms: the value in the type/name/brand column that we are querying for
        thing: search by 'ASC_' | 'DESC_' += 'efficiency'; 'price'; 'percent'; 'ml';
        price_min: minimum price filter (optional)
        price_max: maximum price filter (optional)
    Returns:
        A list of rows from the drinks table matching the search terms
    """
    # conn.create_function('regexp', 2, functionRegex)
    # Create a new cursor
    cur = conn.cursor()
    # Define a new list for which to store our final list of results
    results = list()

    # Split the search keyboards by the spaces in between words
    inputs = terms.split(" ")
    print("SEARCH TERMS: " + str(inputs))
    # return termsList
    terms = list()
    for term in inputs:
        terms.append(term.lower())
    print("-------------------<OLD>-------------------")
    print(terms)
    # now run intellisense search to get better result parity
    print("-------------------(NEW)-------------------")
    search_query = " ".join(terms)
    intelliterms = intellisearch(search_query)
    # print(intelliterms)

    # get the category to search by:
    print(thing)
    parts = thing.split("_")
    category = parts[1]
    order = parts[0]

    # Build price filter conditions
    price_conditions = []
    if price_min:
        price_conditions.append(f"price >= {float(price_min)}")
    if price_max:
        price_conditions.append(f"price <= {float(price_max)}")
    price_filter = ""
    if price_conditions:
        price_filter = " AND " + " AND ".join(price_conditions)

    # For each keyword, execute a new query at the cursor to find drinks matching that keyword
    for term in intelliterms:
        term = term.lower()
        cur.execute(
            "SELECT * FROM drinks WHERE search_text LIKE '%{}%'{} ORDER BY {} {}".format(
                term, price_filter, category, order))

        rows = cur.fetchall()
        print("FOR " + term)

        print("NUMBER OF ROWS FOUND: " + str(len(rows)))
        # For each row in rows, if the row is not already in the results list add it
        for row in rows:
            if row not in results:
                results.append(row)

    print("NUMBER OF RESULTS FOUND: " + str(len(results)))

    # organise results based on category and order - currently only sorted per term.
    if category == 'efficiency':
        if order == 'ASC':
            results.sort(key=lambda tup: tup[10], reverse=False)
        elif order == 'DESC':
            results.sort(key=lambda tup: tup[10], reverse=True)

    elif category == 'price':
        if order == 'ASC':
            results.sort(key=lambda tup: tup[5], reverse=False)
        elif order == 'DESC':
            results.sort(key=lambda tup: tup[5], reverse=True)

    elif category == 'percent':
        if order == 'ASC':
            results.sort(key=lambda tup: tup[8], reverse=False)
        elif order == 'DESC':
            results.sort(key=lambda tup: tup[8], reverse=True)

    elif category == 'ml':
        if order == 'ASC':
            results.sort(key=lambda tup: tup[7], reverse=False)
        elif order == 'DESC':
            results.sort(key=lambda tup: tup[7], reverse=True)



    # Return the final list of results
    return results


def update_drink(conn, drink, newPrice):
    """
    update priority, begin_date, and end date of a task
    :param conn:
    :param drink:
    :return: project id
    """
    sql = ''' UPDATE drinks
              SET price = ?, link = ?, image = ?, efficiency = ?
              WHERE name = ?
              AND brand = ?
              AND store = ? '''

    result = get_drinks_stddrinks(conn, drink)
    if result == False:
        print("failed to update drink... here are the details")
        try:
            print(drink)
        except:
            print("couldnt print drink!")
    else:
        print('---------------')
        print(drink.brand + " " + drink.name)
        cur = conn.cursor()
        cur.execute(sql, (
            newPrice, drink.link, drink.image, float(float(result) / float(newPrice)), drink.name, drink.brand,
            drink.store))
        print(float(newPrice))
        print(float(result))
        print(float(result) / float(newPrice))
        conn.commit()


def is_drink_in_table(conn, drink):
    """
    update priority, begin_date, and end date of a task
    :param conn:
    :param drink:
    :return: project id
    """
    sql = ''' SELECT * FROM drinks
              WHERE store = ?
              AND brand = ?
              AND name = ?
              AND link = ?
              '''
    cur = conn.cursor()
    cur.execute(sql, (drink.store, drink.brand, drink.name, drink.link))

    rows = cur.fetchall()
    if len(rows) > 0:
        return True
    else:
        return False


def get_drinks_stddrinks(conn, drink):
    """
    get the standard drinks of a drink
    :param conn:
    :param drink:
    :return: project id
    """
    sql = ''' SELECT * FROM drinks
              WHERE store = ?
              AND brand = ?
              AND name = ?
              '''
    cur = conn.cursor()
    cur.execute(sql, (drink.store, drink.brand, drink.name))

    rows = cur.fetchall()
    if len(rows) > 0:
        return rows[0][9]
    else:
        return False


def save_short_link(conn, image):
    """
    get the standard drinks of a drink
    :param conn:
    :param drink:
    :return: project id
    """
    sql = ''' UPDATE drinks
              SET shortimage = ?
              WHERE image = ? '''
    cur = conn.cursor()

    if image != None:
        url = str(image)
        oldurl = url
        url = url.replace("/", "~")
        url = url.replace("?", "+")
        url = url.replace(":", ",")
        print(url)
        url = url.split('~')[-1]
        shortimage = url.split("'")[0]
        print(shortimage)
        print(oldurl)
        cur.execute(sql, (shortimage, oldurl))
        conn.commit()
        print('done')


def fix_missing_beer_images(conn):
    """
    get the standard drinks of a drink
    :param conn:
    :param drink:
    :return: project id
    """
    drinks = select_all_drinks(conn)
    emptyImageDrinks = list()

    for drink in drinks:
        print(drink[11])
        if drink[11] is None:
            emptyImageDrinks.append(drink)

    print(len(emptyImageDrinks))

    for empty in emptyImageDrinks:
        for drink in drinks:
            old_empty = empty[3].split("-")[0]
            old_drink = drink[3].split("-")[0]
            if old_empty == old_drink:
                if empty[0] != drink[0]:
                    if empty[2] == drink[2]:
                        if empty[11] is None:
                            if drink[11] is not None:
                                print("--------------------------------")
                                print(drink)
                                print(empty)
                                sql = ''' UPDATE drinks
                                          SET shortimage = ?, image = ?
                                          WHERE name = ? '''
                                cur = conn.cursor()
                                cur.execute(sql, (drink[12], drink[11], empty[3]))
                                conn.commit()


def dbhandler(conn, list, mode, populate):
    # populate or update mode
    if mode == "p":
        for drink in list:
            try:
                price = float(drink.price) if drink.price is not None else 0.0
                ml = float(drink.ml) if drink.ml is not None else 0.0
                percent = float(drink.percent) if drink.percent is not None else 0.0
                std_drinks = float(drink.stdDrinks) if drink.stdDrinks is not None else 0.0
                efficiency = float(drink.efficiency) if drink.efficiency is not None else 0.0
                search_text = build_search_text(drink.name, drink.brand, drink.type)
                
                drink_task = (
                    drink.store, drink.brand, drink.name, drink.type, price, drink.link, ml,
                    percent, std_drinks, efficiency, drink.image, search_text)
                create_entry(conn, drink_task)
            except Exception as e:
                print(f"Error inserting drink {drink.name}: {e}")

    elif mode == "u":
        # update entries with the same name / add entries who's names do not exist.
        for drink in list:
            if is_drink_in_table(conn, drink):
                update_drink(conn, drink, drink.price)

            else:
                if populate:
                    try:
                        price = float(drink.price) if drink.price is not None else 0.0
                        ml = float(drink.ml) if drink.ml is not None else 0.0
                        percent = float(drink.percent) if drink.percent is not None else 0.0
                        std_drinks = float(drink.stdDrinks) if drink.stdDrinks is not None else 0.0
                        efficiency = float(drink.efficiency) if drink.efficiency is not None else 0.0
                        search_text = build_search_text(drink.name, drink.brand, drink.type)
                        
                        drink_task = (drink.store, drink.brand, drink.name, drink.type, price, drink.link,
                                    ml, percent, std_drinks, efficiency,
                                    drink.image, search_text)
                        create_entry(conn, drink_task)
                    except Exception as e:
                        print(f"Error creating drink {drink.name}: {e}")


    conn.commit()


def delete_task(conn, name, brand, store, type):
    """
    Delete a task by task id
    :param conn:  Connection to the SQLite database
    :param id: id of the task
    :return:
    """
    sql = 'DELETE FROM tasks WHERE name=? AND brand=? AND store=? AND type=?'
    cur = conn.cursor()
    cur.execute(sql, (name, brand, store, type))
    conn.commit()


def delete_all(conn):
    """
        Delete a task by task id
        :param conn:  Connection to the SQLite database
        :param id: id of the task
        :return:
        """
    sql = 'DELETE FROM drinks'
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


# ------------------ metrics section ------------------

def total_search(conn):
    sql = 'SELECT * FROM metrics'
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    return len(rows)


def most_common(L):
    # get an iterable of (item, iterable) pairs
    SL = sorted((x, i) for i, x in enumerate(L))
    # print 'SL:', SL
    groups = itertools.groupby(SL, key=operator.itemgetter(0))

    # auxiliary function to get "quality" for an item
    def _auxfun(g):
        item, iterable = g
        count = 0
        min_index = len(L)
        for _, where in iterable:
            count += 1
            min_index = min(min_index, where)
        # print 'item %r, count %r, minind %r' % (item, count, min_index)
        return count, -min_index

    # pick the highest-count/earliest item
    return max(groups, key=_auxfun)[0]


def most_searched(conn):
    sql = 'SELECT * FROM metrics'
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    results = list()
    for row in rows:
        results.append(row[2])
    return most_common(results)
