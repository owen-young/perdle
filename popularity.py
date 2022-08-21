from pywikiapi import wikipedia
from SPARQLWrapper import SPARQLWrapper, JSON
import requests
import json
from datetime import datetime
from contextlib import redirect_stdout
from mwclient import Site
import sqlite3
import functools
import re
from http import HTTPStatus

# Create our database if it does not otherwise exist and update it with
# relevant "measures of popularity."
def update_pop_database():
    # Connect to our local popularity database.
    con = sqlite3.connect("pop.db")
    cur = con.cursor()

    # Create the table if it does not already exist.
    cur.execute('''
            CREATE TABLE IF NOT EXISTS popularity
            (wd_id PRIMARY KEY, wd_label, wp_backlinks, wp_avgviews,
            google_search_num)
            ''')

    # Create a query to update the database containing popularity statistics
    # for each person.
    #
    # https://www.sqlite.org/draft/lang_UPSERT.html
    query = '''
        INSERT INTO popularity (wd_id, wd_label, wp_backlinks, wp_avgviews,
        google_search_num) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(wd_id) DO UPDATE SET
            wp_backlinks = excluded.wp_backlinks,
            wp_avgviews = excluded.wp_avgviews,
            google_search_num = excluded.google_search_num
        '''

    # Loop through all people inside of names.txt and use the following methods
    # to get popularity statistics for our popularity database (pop.db):
    #
    #   1. Use mwclient to access Wikidata to get the Wikipedia page associated
    #      with this person.
    #   2. Use this page name to get the number of backlinks using
    #      http://linkcount.toolforge.org/api/.
    #   3. Get the monthly page view statistics from the start of API data,
    #      July 1st, 2015. Take a monthly average of this data.
    #   4. Search Google for the page name and record how many results there are.
    people_file = open('names.txt', 'r')

    # Connect to English Wikipedia
    wikipedia_site = wikipedia('en')

    # Connect to Wikidata, and get a dictionary of all pages
    wikidata_pages = Site('wikidata.org').pages

    # API endpoint for number of backlinks for a given Wikipedia page.
    backlinks_endp = 'http://linkcount.toolforge.org/api/?page={}&project=en.wikipedia.org'

    # Today's date in a format for Wikidata REST API.
    current_date = datetime.today().strftime('%Y%m%d')

    # REST API endpoint for monthly pageview statistics.
    pageviews_endp = 'https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia.org/all-access/user/{}/monthly/20150701/' + current_date

    # Request headers for Wikidata REST API request.
    req_headers_wiki = {
        'accept': 'application/json',
        'User-Agent': 'Perdle / owen.young0@protonmail.com'
    }

    # Link for Google search
    google_search = 'https://www.google.com/search?q={}'

    # Request headers for Google search
    req_headers_goog = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
    }

    with open('names.txt', 'r') as people_file:
        for person in people_file:
            # Strip the link and new line, leaving only the Wikidata identifier.
            res = re.match(r'http://www\.wikidata\.org/entity/(.*)\n', person)
            if not res or not res.group(1):
                raise RuntimeError('names.txt is malformed!')
            wd_id = res.group(1)

            # Use the Wikidata page identifier to get the page text and convert it to JSON. If
            # Page.text() returns an empty string, then the page doesn't exist. In this case,
            # we print the ID and move on.
            if not (wd_text := wikidata_pages[wd_id].text()):
                print(wd_id, 'does not exist. Move on to the next person.')
                continue

            wd_page = json.loads(wd_text)

            # Get the name of the page on English Wikipedia.
            if not 'enwiki' in wd_page['sitelinks']:
                # If this person does not have an entry on English Wikipedia,
                # sorry, they are getting skipped.
                continue

            wp_page_name = wd_page['sitelinks']['enwiki']['title']

            # Request the number of backlinks for this person via a GET request to a backlinks API.
            # If it fails, print something out and move on.
            response = requests.get(backlinks_endp.format(wp_page_name))
            if response.status_code != HTTPStatus.OK:
                print(wp_page_name, 'could not be requested on backlinks API')
                continue
            num_backlinks = response.json()['wikilinks']['all']

            # Use Wikimedia REST API to get the number of page views for every month since the
            # beginning of pageview API data, July 1, 2015.
            #
            # If a Wikipedia article is too new, such as https://en.wikipedia.org/wiki/Will_Adam
            # (at the time of writing, 8/20/2022, this article was too new for statistics), then a
            # bad status (404) will come back from HTTP.
            response = requests.get(pageviews_endp.format(wp_page_name), headers=req_headers_wiki)

            if response.status_code != HTTPStatus.OK:
                print(wp_page_name, 'could not be found, or is too new for statistics for the requested range. Move on')
                continue

            # Compute a monthly average.
            avg_pageviews = functools.reduce(lambda acc, el: acc + el['views'], \
                                             response.json()['items'], 0) / len(response.json()['items'])

            # Get the number of Google results returned by the name in question. This relies on
            # the fact that Google puts how many results there are in the "result-stats" div.
            response = requests.get(google_search.format(wp_page_name), headers=req_headers_goog)

            if response.status_code != HTTPStatus.OK:
                print(wp_page_name, 'couldn\'t complete the request to Google. Ouch')
                continue

            # Extract the number of results from the HTML string, separated by commas
            res = re.search(r'About ([0-9,]+) results', response.text)
            if not res or not res.group(1):
                raise RuntimeError('Results string not found in Google HTML!')

            num_goog_results = int(res.group(1).replace(',', ''))

            # Finally, we can put all of these statistics into our database. Execute the query.
            cur.execute(query, (wd_id, wp_page_name, num_backlinks, avg_pageviews, num_goog_results))

    # We're out of names. Commit the transaction, close the connection, and return.
    con.commit()
    con.close()
    return

update_pop_database()






