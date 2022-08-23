from pywikiapi import wikipedia
import requests
from datetime import datetime
import sqlite3
import functools
import re
from http import HTTPStatus
from concurrent.futures import ThreadPoolExecutor
from SPARQLWrapper import SPARQLWrapper, JSON

# Update our database, pop.db, with all popularity statistics. This code runs
# with the following assumptions:
#
# The database exists and is populated with rows for every human on Wikidata,
# with the wd_article column filled with its associated English Wikipedia article
# name.

# Return a list of tuples of the format (wd_id, wd_sitelinks) to UPDATE when this
# thread completes.
#
# Input: all_rows is a list of tuples, where each tuple is a row in the popularity
#        database. This is of the format (wd_id, wp_article).
def update_sitelinks(all_rows):

    # SPARQL query to get the number of Wiki sitelinks for a given person
    sitelinks_query = '''
    SELECT ?sitelinks
    WHERE {{
      wd:{} wikibase:sitelinks ?sitelinks .
    }}
    '''

    # Define a list tuples to be UPDATEd once this thread finishes.
    tuple_list = []

    # Create a SPARQLWrapper for performing SPARQL SELECT requests.
    sparql = SPARQLWrapper('https://query.wikidata.org/sparql')
    sparql.setReturnFormat(JSON)

    for row in all_rows:
        wd_id = row[0]

        # Get the number of Wiki sitelinks for this page using Wikidata SPARQL endpoint.
        sparql.setQuery(sitelinks_query.format(wd_id))
        ret = sparql.queryAndConvert()

        # Make sure we didn't get more than one sitelink value from the SPARQL endpoint.
        if len(ret['results']['bindings']) != 1:
            raise RuntimeError('More than one sitelinks count:', ret['results']['bindings'])

        json_sitelink = ret['results']['bindings'][0]['sitelinks']

        # Get the number of sitelinks from the JSON response. If the response we got was not
        # an integer, something is wrong and this script must be changed.
        if json_sitelink['datatype'] != 'http://www.w3.org/2001/XMLSchema#integer':
            raise RuntimeError('Sitelinks data type is not an integer:',
                                json_sitelink['datatype'])

        wd_sitelinks = int(json_sitelink['value'])

        # Add this tuple to the list to UPDATE.
        tuple_list.append((wd_sitelinks, wd_id))

    return tuple_list


# Return a list of tuples of the format (wd_id, wp_backlinks) to UPDATE when this
# thread completes.
#
# Input: all_rows is a list of tuples, where each tuple is a row in the popularity
#        database. This is of the format (wd_id, wp_article).
def update_backlinks(all_rows):

    # API endpoint for number of backlinks for a given Wikipedia page.
    backlinks_endp = 'http://linkcount.toolforge.org/api/?page={}&project=en.wikipedia.org'

    # Define a list tuples to be UPDATEd once this thread finishes.
    tuple_list = []

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

        # Add this tuple to the list to UPDATE.
        tuple_list.append((num_backlinks, wd_id))

    return tuple_list

# Return a list of tuples of the format (wd_id, wp_avgviews) to UPDATE when this
# thread completes.
#
# Input: all_rows is a list of tuples, where each tuple is a row in the popularity
#        database. This is of the format (wd_id, wp_article).
def update_avgviews(all_rows):

    # Today's date in a format for Wikidata REST API.
    current_date = datetime.today().strftime('%Y%m%d')

    # REST API endpoint for monthly pageview statistics.
    pageviews_endp = 'https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia.org/all-access/user/{}/monthly/20150701/' + current_date

    # Request headers for Wikidata REST API request.
    req_headers_wiki = {
        'accept': 'application/json',
        'User-Agent': 'Perdle / owen.young0@protonmail.com'
    }

    # Define a list tuples to be UPDATEd once this thread finishes.
    tuple_list = []

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
        avg_pageviews = functools.reduce(lambda acc, el: acc + el['views'],
                                        response.json()['items'], 0) / len(response.json()['items'])

        # Add this tuple to the list to UPDATE.
        tuple_list.append((avg_pageviews, wd_id))

    return tuple_list

# Return a list of tuples of the format (wd_id, google_search_num) to UPDATE when this
# thread completes.
#
# Input: all_rows is a list of tuples, where each tuple is a row in the popularity
#        database. This is of the format (wd_id, wp_article).
def update_goog_search_num(all_rows):

    # Link for Google search
    google_search = 'https://www.google.com/search?q={}'

    # Request headers for Google search
    req_headers_goog = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
    }

    # Define a list tuples to be UPDATEd once this thread finishes.
    tuple_list = []

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

        # Add this tuple to the list to UPDATE.
        tuple_list.append((num_goog_results, wd_id))

    return tuple_list

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
    # We should not have concurrency issues (fingers crossed!) in the database because
    # each thread is updating a separate column of any given row. The order that these
    # transactions occur in does not matter, as long as each column is updated.

    with ThreadPoolExecutor() as executor:
        sitelinks_thread = executor.submit(update_sitelinks, all_rows=all_db_rows)
        backlinks_thread = executor.submit(update_backlinks, all_rows=all_db_rows)
        pageviews_thread = executor.submit(update_avgviews, all_rows=all_db_rows)
        goog_search_thread = executor.submit(update_goog_search_num, all_rows=all_db_rows)

        # Check each thread for exceptions. Each function returns None.
        try:
            # Query to update the number of Wiki sitelinks for a given person
            sitelinks_upd_query = 'UPDATE popularity SET wd_sitelinks = ? WHERE wd_id = ?'
            tuple_list = sitelinks_thread.result()

            # Update the database.
            cur.executemany(sitelinks_upd_query, tuple_list)
        except Exception as e:
            print("Sitelinks thread failed with the following exception:", e)

        try:
            # Query to update the number of backlinks for a given person
            bl_query = 'UPDATE popularity SET wp_backlinks = ? WHERE wd_id = ?'
            tuple_list = backlinks_thread.result()

            # Update the database.
            cur.executemany(bl_query, tuple_list)
        except Exception as e:
            print("Backlinks thread failed with the following exception:", e)

        try:
            # Query to update the number of backlinks for a given person
            avg_views_query = 'UPDATE popularity SET wp_avgviews = ? WHERE wd_id = ?'
            tuple_list = pageviews_thread.result()

            # Update the database.
            cur.executemany(avg_views_query, tuple_list)
        except Exception as e:
            print("Pageviews thread failed with the following exception:", e)
        try:
            # Query to update the number of backlinks for a given person
            search_num_query = 'UPDATE popularity SET google_search_num = ? WHERE wd_id = ?'
            tuple_list = goog_search_thread.result()

            # Update the database.
            cur.executemany(search_num_query, tuple_list)
        except Exception as e:
            print("Google search thread failed with the following exception", e)

    con.commit()
    con.close()

    return

driver()