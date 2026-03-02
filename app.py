from flask import Flask
from flask import render_template
from flask import request
from flask import redirect
from flask import current_app
from flask import jsonify
from datetime import datetime
import re
import json
from urllib.request import urlopen
import ipinfo
import random
from config import Config

# from scrape2 import search
import db.databaseHandler as db

# Create a new flask application
app = Flask(__name__)

# Working serve of the search page with css and js
@app.route("/")
def displaySearchPage():
    """
    ...
    """
    # Get the current top drink from the database
    conn = db.create_connection()  # connect to the database
    topDrink = db.select_all_drinks_by_efficiency(conn)[0] # get the first result from all of the drinks sorted by efficiency desc
    return render_template('index.html', result=topDrink)

# A function to get search terms from the search page
@app.route('/', methods=['POST'])
def postSearchTerms():
    """
    ...
    """
    # Get the search terms inputted by the user
    searchTerms = request.form['searchTerms']
    print("SEARCH TERMS ENTERED BY USER: " + searchTerms)

    # Send the user to the results page
    return redirect(f"/search?q={searchTerms}&order=score-desc")

ORDER_MAP = {
    "score-desc": "DESC_efficiency",
    "score-asc": "ASC_efficiency",
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
        order: sort order (default: score-desc)
        page: page number (default: 1)
    """
    search_terms = request.args.get("q", "")
    order_param = request.args.get("order", "score-desc")
    try:
        page = int(request.args.get("page", 1))
    except ValueError:
        page = 1

    per_page = 16
    sort_key = ORDER_MAP.get(order_param, "DESC_efficiency")

    conn = db.create_connection()
    all_results = db.select_drink_by_smart_search(conn, search_terms, sort_key)
    
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
    tempResults = db.select_drink_by_smart_search(conn, "beer", 'DESC_efficiency')
    tempResults = insert_ads_amongst_results(tempResults[:50])

    # gather metrics info
    metrics(["beer"])
    return render_template('top50.html', results=tempResults)

@app.route("/top50/wine")
def display_top50wine_page():
    # Get results the new way - by querying the database
    conn = db.create_connection()  # connect to the database
    tempResults = db.select_drink_by_smart_search(conn, "wine", 'DESC_efficiency')
    tempResults = insert_ads_amongst_results(tempResults[:50])

    # gather metrics info
    metrics(["wine"])
    return render_template('top50.html', results=tempResults)

@app.route("/top50/spirits")
def display_top50spirits_page():
    # Get results the new way - by querying the database
    conn = db.create_connection()  # connect to the database
    tempResults = db.select_drink_by_smart_search(conn, "spirits", 'DESC_efficiency')
    tempResults = insert_ads_amongst_results(tempResults[:50])

    # gather metrics info
    metrics(["spirits"])
    return render_template('top50.html', results=tempResults)

# Handle search form submission from results page
@app.route("/search", methods=["POST"])
def search_post():
    """Handle search form submission."""
    search_terms = request.form.get("searchTerms", "")
    order = request.form.get("order", "score-desc")
    return redirect(f"/search?q={search_terms}&order={order}")


# Route for About Us page
# @app.route('/about', methods=['GET', 'POST'])
# def viewabout():
#     return render_template('About.html')  # render a template

# Route for About Us page
@app.route('/faq', methods=['GET', 'POST'])
def viewFAQ():
    return render_template('FAQ.html')  # render a template

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
        tempResults = db.select_drink_by_smart_search(conn, term, 'DESC_efficiency')
    elif order == "score_asc":
        tempResults = db.select_drink_by_smart_search(conn, term, 'ASC_efficiency')
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
        drink['efficiency'] = result[10]
        drink['imglink'] = result[11]
        drink['img'] = result[12]
        data[i] = drink
        i = i + 1

    # return json_data
    return current_app.response_class(json.dumps(data), mimetype="application/json")
    # return jsonify({'ip': request.remote_addr}), 200


# Run the flask application (won't run when the site is being hosted on a server)
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
    #app.run(host='127.0.0.1', port=8000, debug=True)
