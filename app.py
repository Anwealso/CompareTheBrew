from flask import Flask
from flask import render_template
from flask import request
from flask import redirect
from flask import current_app
from flask import jsonify
from datetime import datetime
import re
import json
import argparse
from urllib.request import urlopen
import ipinfo
import random
from config import Config

# from scrape2 import search
import db.databaseHandler as db

# Create a new flask application
app = Flask(__name__)
app.config.from_object(Config)

@app.template_filter('staleness')
def staleness_filter(date_str):
    if not date_str:
        return {'label': '?', 'class': 'freshness-unknown'}
    try:
        date_obj = None
        formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f']
        if isinstance(date_str, str):
            for fmt in formats:
                try:
                    date_obj = datetime.strptime(date_str, fmt)
                    break
                except ValueError:
                    continue
        elif isinstance(date_str, datetime):
            date_obj = date_str
        
        if date_obj is None:
            return {'label': '?', 'class': 'freshness-unknown'}
        
        days_old = (datetime.now().date() - date_obj.date()).days
        if days_old <= 0:
            return {'label': 'today', 'class': 'freshness-today'}
        elif days_old == 1:
            return {'label': '1 day ago', 'class': 'freshness-recent'}
        elif days_old >= 2 and days_old <= 6:
            return {'label': '2-6 days ago', 'class': 'freshness-recent'}
        elif days_old >= 7 and days_old <= 13:
            return {'label': 'a week ago', 'class': 'freshness-week'}
        else:
            return {'label': 'more than a week ago', 'class': 'freshness-stale'}
    except (ValueError, TypeError, AttributeError):
        return {'label': '?', 'class': 'freshness-unknown'}

@app.context_processor
def inject_today():
    return {'today_date': datetime.now().date()}

@app.context_processor
def inject_feature_flags():
    return {'flag_show_staleness': app.config.get('FLAG_SHOW_STALENESS', True)}


def parse_zero_alc_flag(value):
    """
    Normalize a request value to a boolean indicating whether the zero-alcohol
    filter is active.
    """
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1", "yes", "on")

@app.route("/")
def displaySearchPage():
    """
    ...
    """
    # Get the current top drink from the database
    conn = db.create_connection()  # connect to the database
    top_results = db.select_all_drinks_by_score(conn)
    topDrink = top_results[0] if top_results else None
    return render_template('index.html', result=topDrink)

@app.route('/', methods=['POST'])
def postSearchTerms():
    """
    A function to get search terms from the search page
    """
    # Get the search terms inputted by the user
    searchTerms = request.form['searchTerms']
    print("SEARCH TERMS ENTERED BY USER: " + searchTerms)
    zero_alc_flag = parse_zero_alc_flag(request.form.get("zero-alc"))

    # Send the user to the results page
    url = f"/search?q={searchTerms}&order=score-asc"
    if zero_alc_flag:
        url += "&zero-alc=true"
    return redirect(url)

ORDER_MAP = {
    "score-desc": "DESC_score",
    "score-asc": "ASC_score",
    "price-desc": "DESC_price",
    "price-asc": "ASC_price",
    "size-desc": "DESC_ml",
    "size-asc": "ASC_ml",
    "percent-desc": "DESC_percent",
}


@app.route("/search")
def search_page():
    """
    Unified search page with query parameters.

    Args:
        q: search query (required)
        order: sort order (default: score-asc)
        page: page number (default: 1)
        price_min: minimum price filter (optional)
        price_max: maximum price filter (optional)
        store: retailer filter (optional, default: all)
    """
    search_terms = request.args.get("q", "")
    order_param = request.args.get("order", "score-asc")
    try:
        page = int(request.args.get("page", 1))
    except ValueError:
        page = 1

    price_min = request.args.get("price_min", "")
    price_max = request.args.get("price_max", "")
    store_filter = request.args.get("store", "all")
    scraped_age = request.args.get("scraped_age", "")
    zero_alc_filter = parse_zero_alc_flag(request.args.get("zero-alc", ""))
    if scraped_age is not None and scraped_age != "":
        try:
            scraped_age = int(scraped_age)
        except ValueError:
            scraped_age = None
    else:
        scraped_age = None

    per_page = 16
    sort_key = ORDER_MAP.get(order_param, "ASC_score")

    conn = db.create_connection()
    all_results = db.select_drink_by_smart_search(
        conn, search_terms, sort_key, price_min, price_max, store_filter, scraped_age, zero_alc_filter
    )
    
    # Insert ads into the full list before paginating, or just into the page?
    # Usually better to insert ads into the full list so they stay in consistent positions,
    # but for simplicity and to ensure some ads per page, we can do it per page.
    # However, the user asked to "randomly insert some advertisement cards".
    
    total_results_count = len(all_results)
    total_pages = (total_results_count + per_page - 1) // per_page

    # Slice for the current page
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    page_results = all_results[start_idx:end_idx]

    # Insert ads into this page's results
    if page_results:
        page_results = insert_ads_amongst_results(page_results)

    metrics(search_terms)

    return render_template(
        "results.html",
        results=page_results,
        search_terms=search_terms,
        order=order_param,
        current_page=page,
        total_pages=total_pages,
        total_results=total_results_count,
        price_min=price_min,
        price_max=price_max,
        store_filter=store_filter,
        scraped_age=scraped_age,
        zero_alc_filter=zero_alc_filter,
        zero_alc_query="&zero-alc=true" if zero_alc_filter else ""
    )


# Legacy redirects for old URLs
@app.route("/results=<order>/<searchTerms>")
def legacy_search_redirect(order, searchTerms):
    return redirect(f"/search?q={searchTerms}&order={order}")


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#                 advert insert
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def insert_ads_amongst_results(tempResults):
    num_ads = 0  # how many ads are currently added to the list
    next_ad_index = 1  # the index we will place the next ad item at
    drinks_per_ad = 5  # how many legitimate drinks cards (-1) we will have until we show the next drink card  i.e. 3 = 1 ad per 3 drinks
    while next_ad_index < len(tempResults):  # While we have not yet finished putting ads all through the list
        tempResults.insert(next_ad_index, ['GOOGLE_AD'])  # Add an advertisement item to the list
        num_ads = num_ads + 1  # increment the number of ads we have added to the page
        next_ad_index = (num_ads * drinks_per_ad) + random.randint(1,
                                                                   drinks_per_ad)  # calculate the position in which we will put the next drink card
    return tempResults


def metrics(searchTerms):
    try:
        TOQ = datetime.now().strftime('%H:%M:%S %Y-%m-%d')
        print(TOQ)
        query = ""
        for term in searchTerms:
            query += term
        print(query)
        access_token = Config.IPINFO_TOKEN
        handler = ipinfo.getHandler(access_token)
        IP = ""
        if request.environ.get('HTTP_X_FORWARDED_FOR') is None:
            IP = request.environ['REMOTE_ADDR']
        else:
            IP = request.environ['HTTP_X_FORWARDED_FOR']  # if behind a proxy
        print(IP)
        details = handler.getDetails(IP)
        print(details.all)
        hostname = ""
        print(hostname)
        org = ""
        print(org)
        city = ""
        print(city)
        country = details.country_name
        print(country)
        region = ""
        lat = details.latitude
        long = details.longitude

        print(region)
        print(lat)
        print(long)

        metconn = db.create_metrics_connection()
        metric = (
            str(IP), str(query), str(TOQ), str(country), str(region), str(city), float(lat), float(long), str(hostname),
            str(org))
        ID = db.create_metric_entry(metconn, metric)
        print("ID: " + str(ID))
    except Exception as e:
        print(e)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#            top50page
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
@app.route("/top50/beer")
def display_top50_page():
    # Get results the new way - by querying the database
    conn = db.create_connection()  # connect to the database
    tempResults = db.select_drink_by_smart_search(conn, "beer", 'ASC_score')
    tempResults = insert_ads_amongst_results(tempResults[:50])

    # gather metrics info
    metrics(["beer"])
    return render_template('top50.html', results=tempResults)
@app.route("/top50/wine")
def display_top50wine_page():
    # Get results the new way - by querying the database
    conn = db.create_connection()  # connect to the database
    tempResults = db.select_drink_by_smart_search(conn, "wine", 'ASC_score')
    tempResults = insert_ads_amongst_results(tempResults[:50])

    # gather metrics info
    metrics(["wine"])
    return render_template('top50.html', results=tempResults)
@app.route("/top50/spirits")
def display_top50spirits_page():
    # Get results the new way - by querying the database
    conn = db.create_connection()  # connect to the database
    tempResults = db.select_drink_by_smart_search(conn, "spirits", 'ASC_score')
    tempResults = insert_ads_amongst_results(tempResults[:50])

    # gather metrics info
    metrics(["spirits"])
    return render_template('top50.html', results=tempResults)
# Handle search form submission from results page
@app.route("/search", methods=["POST"])
def search_post():
    """Handle search form submission."""
    search_terms = request.form.get("searchTerms", "")
    order = request.form.get("order", "score-asc")
    zero_alc_flag = parse_zero_alc_flag(request.form.get("zero-alc"))
    url = f"/search?q={search_terms}&order={order}"
    if zero_alc_flag:
        url += "&zero-alc=true"
    return redirect(url)


# Route for About Us page
# @app.route('/about', methods=['GET', 'POST'])
# def viewabout():
#     return render_template('about.html')  # render a template

# Route for About Us page
@app.route('/faq', methods=['GET', 'POST'])
def viewFAQ():
    return render_template('FAQ.html')

# Ajunner Error Handling
# 404
@app.errorhandler(404)
def page_not_found404(e):
    return render_template('/404.html'), 404

# 500
@app.errorhandler(500)
def page_not_found500(e):
    return render_template('/500.html'), 500

@app.route('/api', methods=['GET', 'POST'])
def api_handler():
    term = request.args.get('term')
    order = request.args.get('order')
    print(term)
    print(order)

    # Get results the new way - by querying the database
    conn = db.create_connection()  # connect to the database
    tempResults = []
    # gather metrics info
    metrics(term)

    if order == "score_desc":
        tempResults = db.select_drink_by_smart_search(conn, term, 'DESC_score')
    elif order == "score_asc":
        tempResults = db.select_drink_by_smart_search(conn, term, 'ASC_score')
    elif order == "price_desc":
        tempResults = db.select_drink_by_smart_search(conn, term, 'DESC_price')
    elif order == "size_desc":
        tempResults = db.select_drink_by_smart_search(conn, term, 'DESC_ml')

    print(tempResults)

    data = {}

    # loop over tuples
    i = 0
    for result in tempResults:
        drink = dict()
        drink['id'] = result[0]
        drink['store'] = result[1]
        drink['brand'] = result[2]
        drink['name'] = result[3]
        drink['type'] = result[4]
        drink['price'] = result[5]
        drink['url'] = result[6]
        drink['volume'] = result[7]
        drink['percent'] = result[8]
        drink['drinks'] = result[9]
        drink['score'] = result[11]
        drink['imglink'] = result[12]
        drink['img'] = result[13]
        data[i] = drink
        i = i + 1

    # return json_data
    return current_app.response_class(json.dumps(data), mimetype="application/json")
    # return jsonify({'ip': request.remote_addr}), 200


# Run the flask application (won't run when the site is being hosted on a server)
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=80, help='Port to run the server on')
    args = parser.parse_args()
    app.run(host='0.0.0.0', port=args.port, debug=True)
