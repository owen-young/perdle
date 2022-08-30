from pywikiapi import wikipedia
from datetime import datetime
import sqlite3
import functools
import re
from http import HTTPStatus
import aiohttp
import asyncio

# Update our database, pop.db, with all popularity statistics. This code runs
# with the following assumptions:
#
# The database exists and is populated with rows for every human on Wikidata,
# with the wd_article column filled with its associated English Wikipedia article
# name.
#
# NOTE: I know it is bad to use .format() for SQL queries because of SQL injection
#       attacks. However, I figured since each input is something that is already
#       in the database, and this takes no user input (it only reads from pop.db),
#       that threat is not as serious. I could be wrong, but hey.

# Return a SQL UPDATE query for the number of Wiki sitelinks.
#
# Input: row is a tuple of the format (wd_id, wp_article).
#
#        session is the aiohttp ClientSession associated with this asynchronous
#        API request.
async def update_sitelinks(row, session):

    # SPARQL query to get the number of Wiki sitelinks for a given person
    sitelinks_query = '''
    SELECT ?sitelinks
    WHERE {{
      wd:{} wikibase:sitelinks ?sitelinks .
    }}
    '''

    # SPARQL endpoint
    url = 'https://query.wikidata.org/sparql'

    wd_id = row[0]

    # Define the parameters for upcoming GET request.
    wiki_params = {'format': 'json', 'query': sitelinks_query.format(wd_id)}

    # Query to update the number of Wiki sitelinks for a given person
    sitelinks_upd_query = 'UPDATE popularity SET wd_sitelinks = {} WHERE wd_id = {}'

    # Perform the GET request for the number of sitelinks for this person.
    async with session.get(url, params=wiki_params) as resp:

        ret = await resp.json()

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

    # Return a query
    return sitelinks_upd_query.format(wd_sitelinks, wd_id)


# Return a SQL UPDATE query for the number of backlinks.
#
# Input: row is a tuple of the format (wd_id, wp_article).
#
#        session is the aiohttp ClientSession associated with this asynchronous
#        API request.
async def update_backlinks(row, session):

    # API endpoint for number of backlinks for a given Wikipedia page.
    backlinks_endp = 'http://linkcount.toolforge.org/api/?page={}&project=en.wikipedia.org'

    # Query to update the number of backlinks for a given person
    bl_query = 'UPDATE popularity SET wp_backlinks = {} WHERE wd_id = {}'

    wd_id = row[0]
    wp_article = row[1]

    # Request the number of backlinks for this person via a GET request to a backlinks API.
    async with session.get(backlinks_endp.format(wp_article)) as resp:

        ret = await resp.json()

        # If we got a bad status, skip updating it.
        if resp.status != HTTPStatus.OK:
            print(wp_article, 'could not be requested on backlinks API')
            return ''

        num_backlinks = ret['wikilinks']['all']

    return bl_query.format(num_backlinks, wd_id)

# Return a SQL UPDATE query for the average number of pageviews.
#
# Input: row is a tuple of the format (wd_id, wp_article).
#
#        session is the aiohttp ClientSession associated with this asynchronous
#        API request.
async def update_avgviews(row, session):

    # Today's date in a format for Wikidata REST API.
    current_date = datetime.today().strftime('%Y%m%d')

    # REST API endpoint for monthly pageview statistics.
    pageviews_endp = 'https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia.org/all-access/user/{}/monthly/20150701/' + current_date

    # Request headers for Wikidata REST API request.
    req_headers_wiki = {
        'accept': 'application/json',
        'User-Agent': 'Perdle / owen.young0@protonmail.com'
    }

    # Query to update the number of backlinks for a given person
    avg_views_query = 'UPDATE popularity SET wp_avgviews = {} WHERE wd_id = {}'

    wd_id = row[0]
    wp_article = row[1]

    # Use Wikimedia REST API to get the number of page views for every month since the
    # beginning of pageview API data, July 1, 2015.
    #
    # If a Wikipedia article is too new, such as https://en.wikipedia.org/wiki/Will_Adam
    # (at the time of writing, 8/21/2022, this article was too new for statistics), then a
    # bad status (404) will come back from HTTP.
    async with session.get(pageviews_endp.format(wp_article), headers=req_headers_wiki) as resp:

        ret = await resp.json()

        if resp.status != HTTPStatus.OK:
            print(wp_article, 'could not be found, or is too new for statistics for the requested range. Move on')
            return ''

        # Compute a monthly average.
        avg_pageviews = functools.reduce(lambda acc, el: acc + el['views'],
                                        ret['items'], 0) / len(ret['items'])

    return avg_views_query.format(avg_pageviews, wd_id)

# Return a SQL UPDATE query for the number of Google search results.
#
# Input: row is a tuple of the format (wd_id, wp_article).
#
#        session is the aiohttp ClientSession associated with this asynchronous
#        API request.
async def update_goog_search_num(row, session):

    # Link for Google search
    google_search = 'https://www.google.com/search?q={}'

    # Request headers for Google search
    req_headers_goog = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
    }

    # Query to update the number of backlinks for a given person
    search_num_query = 'UPDATE popularity SET google_search_num = {} WHERE wd_id = {}'

    wd_id = row[0]
    wp_article = row[1]

    # Get the number of Google results returned by the name in question. This relies on
    # the fact that Google puts how many results there are in the "result-stats" div.
    async with session.get(google_search.format(wp_article), headers=req_headers_goog) as resp:

        ret = await resp.text()

        if resp.status != HTTPStatus.OK:
            print(wp_article, 'couldn\'t complete the request to Google. Ouch')
            return ''

        # Extract the number of results from the HTML string, separated by commas
        res = re.search(r'About ([0-9,]+) results', ret)
        if not res or not res.group(1):
            print(ret)
            raise RuntimeError('Results string not found in Google HTML!')

        num_goog_results = int(res.group(1).replace(',', ''))

    return search_num_query.format(num_goog_results, wd_id)

async def driver():

    # Connect to our local popularity database.
    con = sqlite3.connect("pop.db")
    cur = con.cursor()

    # Get all rows and pass them to each thread.
    res = cur.execute('SELECT wd_id, wp_article FROM popularity')
    all_db_rows = res.fetchall()

    con.close()

    # Asynchronously perform each API request and UPDATE the popularity database.
    async with aiohttp.ClientSession() as session:
        tasks = []

        # Ensure 4 futures for each person in the database, where each return is
        # a string query to be executed.
        for row in all_db_rows:
            tasks.append(asyncio.ensure_future(update_sitelinks(row, session)))
            tasks.append(asyncio.ensure_future(update_backlinks(row, session)))
            tasks.append(asyncio.ensure_future(update_avgviews(row, session)))
            tasks.append(asyncio.ensure_future(update_goog_search_num(row, session)))

        # Get the queries from each function and execute them.
        queries = await asyncio.gather(*tasks)

        for query in queries:
            cur.execute(query)

    # We're done, commit the transaction, close the database, and return.
    con.commit()
    con.close()

    return

asyncio.run(driver())