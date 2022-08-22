from pywikiapi import wikipedia
import requests
from datetime import datetime
import sqlite3
import functools
import re
from http import HTTPStatus
from concurrent.futures import ThreadPoolExecutor

# Update our database, pop.db, with all popularity statistics. This code runs
# with the following assumptions:
#
# The database exists and is populated with rows for every human on Wikidata,
# with the wd_article column filled with its associated English Wikipedia article
# name.


# Update all backlinks in the popularity database.
#
# Input: all_rows is a list of tuples, where each tuple is a row in the popularity
#        database. This is of the format (wd_id, wp_article).
def update_backlinks(all_rows):

    # Connect to our local popularity database.
    con = sqlite3.connect("pop.db")
    cur = con.cursor()

    # Query to update the number of backlinks for a given person
    bl_query = 'UPDATE popularity SET wp_backlinks = ? WHERE wd_id = ?'

    # API endpoint for number of backlinks for a given Wikipedia page.
    backlinks_endp = 'http://linkcount.toolforge.org/api/?page={}&project=en.wikipedia.org'

    for row in all_rows:
        wd_id = row[0]
        wp_article = row[1]

        # Request the number of backlinks for this person via a GET request to a backlinks API.
        response = requests.get(backlinks_endp.format(wp_article))

        # If we got a bad status, skip updating it.
        if response.status_code != HTTPStatus.OK:
            print(wp_article, 'could not be requested on backlinks API')
            continue

        num_backlinks = response.json()['wikilinks']['all']

        # Update the database.
        cur.execute(bl_query, (num_backlinks, wd_id))

    # We have reached the end of the database. Commit the changes, close the connection,
    # and exit.
    con.commit()
    con.close()
    return

# Update all average page view statistics in the popularity database.
#
# Input: all_rows is a list of tuples, where each tuple is a row in the popularity
#        database. This is of the format (wd_id, wp_article).
def update_avgviews(all_rows):

    # Connect to our local popularity database.
    con = sqlite3.connect("pop.db")
    cur = con.cursor()

    # Query to update the number of backlinks for a given person
    avg_views_query = 'UPDATE popularity SET wp_avgviews = ? WHERE wd_id = ?'

    # Today's date in a format for Wikidata REST API.
    current_date = datetime.today().strftime('%Y%m%d')

    # REST API endpoint for monthly pageview statistics.
    pageviews_endp = 'https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia.org/all-access/user/{}/monthly/20150701/' + current_date

    # Request headers for Wikidata REST API request.
    req_headers_wiki = {
        'accept': 'application/json',
        'User-Agent': 'Perdle / owen.young0@protonmail.com'
    }

    for row in all_rows:
        wd_id = row[0]
        wp_article = row[1]

        # Use Wikimedia REST API to get the number of page views for every month since the
        # beginning of pageview API data, July 1, 2015.
        #
        # If a Wikipedia article is too new, such as https://en.wikipedia.org/wiki/Will_Adam
        # (at the time of writing, 8/21/2022, this article was too new for statistics), then a
        # bad status (404) will come back from HTTP.
        response = requests.get(pageviews_endp.format(wp_article), headers=req_headers_wiki)

        if response.status_code != HTTPStatus.OK:
            print(wp_article, 'could not be found, or is too new for statistics for the requested range. Move on')
            continue

        # Compute a monthly average.
        avg_pageviews = functools.reduce(lambda acc, el: acc + el['views'], \
                                        response.json()['items'], 0) / len(response.json()['items'])

        # Update the database.
        cur.execute(avg_views_query, (avg_pageviews, wd_id))

    # We have reached the end of the database. Commit the changes, close the connection,
    # and exit.
    con.commit()
    con.close()
    return

# Update all google search results statistics in the popularity database.
#
# Input: all_rows is a list of tuples, where each tuple is a row in the popularity
#        database. This is of the format (wd_id, wp_article).
def update_goog_search_num(all_rows):

    # Connect to our local popularity database.
    con = sqlite3.connect("pop.db")
    cur = con.cursor()

    # Query to update the number of backlinks for a given person
    search_num_query = 'UPDATE popularity SET google_search_num = ? WHERE wd_id = ?'

    # Link for Google search
    google_search = 'https://www.google.com/search?q={}'

    # Request headers for Google search
    req_headers_goog = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
    }

    for row in all_rows:
        wd_id = row[0]
        wp_article = row[1]

        # Get the number of Google results returned by the name in question. This relies on
        # the fact that Google puts how many results there are in the "result-stats" div.
        response = requests.get(google_search.format(wp_article), headers=req_headers_goog)

        if response.status_code != HTTPStatus.OK:
            print(wp_article, 'couldn\'t complete the request to Google. Ouch')
            continue

        # Extract the number of results from the HTML string, separated by commas
        res = re.search(r'About ([0-9,]+) results', response.text)
        if not res or not res.group(1):
            raise RuntimeError('Results string not found in Google HTML!')

        num_goog_results = int(res.group(1).replace(',', ''))

        # Update the database.
        cur.execute(search_num_query, (num_goog_results, wd_id))

    # We have reached the end of the database. Commit the changes, close the connection,
    # and exit.
    con.commit()
    con.close()
    return

def driver():
    # Connect to our local popularity database.
    con = sqlite3.connect("pop.db")
    cur = con.cursor()

    # Get all rows and pass them to each thread.
    res = cur.execute('SELECT wd_id, wp_article FROM popularity')
    all_db_rows = res.fetchall()

    con.close()

    # Start a thread for each field in the database that needs to be updated.
    #
    # We should not have concurrency issues in the database because each thread
    # is updating a separate column of any given row. The order that these
    # transactions occur in does not matter, as long as each column is updated.

    with ThreadPoolExecutor() as executor:
        backlinks_thread = executor.submit(update_backlinks, all_rows=all_db_rows)
        pageviews_thread = executor.submit(update_avgviews, all_rows=all_db_rows)
        goog_search_thread = executor.submit(update_goog_search_num, all_rows=all_db_rows)

        # Check each thread for exceptions. Each function returns None.
        try:
            backlinks_thread.result()
        except Exception as e:
            print("Backlinks thread failed with the following exception:", e)

        try:
            pageviews_thread.result()
        except Exception as e:
            print("Pageviews thread failed with the following exception:", e)

        try:
            goog_search_thread.result()
        except Exception as e:
            print("Google search thread failed with the following exception")

    return

driver()