import sqlite3
import re
from mwclient import Site
import json
from SPARQLWrapper import SPARQLWrapper, JSON

def init_db():
    # Create our popularity database and populate it with People from Wikidata.

    con = sqlite3.connect("pop.db")
    cur = con.cursor()

    # Create the table if it does not already exist, which contains:
    #    wd_id (primary key): name on Wikidata, such as Q747.
    #    wp_article:          artice name on English Wikipedia.
    #    wp_backlinks:        the number of backlinks on English Wikipedia for this person.
    #    wp_avgviews:         Average monthly views starting July 2015 for this
    #                         English Wikipedia article.
    #    google_search_num:   Number of Google Search results for this English Wikipedia
    #                         article name.
    cur.execute('''
                CREATE TABLE IF NOT EXISTS popularity
                (wd_id PRIMARY KEY, wp_article, wp_backlinks, wp_avgviews,
                google_search_num)
                ''')

    # Create a query to insert a row for each person in names.txt.
    query = '''
            INSERT INTO popularity (wd_id, wp_article) VALUES (?, ?)
            '''

    # Create a SPARQL query to get all humans. Unfortunately, I would have done a query
    # that only includes humans on the English Wikipedia, however, both the XML and
    # JSON parser failed when trying to parse the output. RDF cannot be requested for
    # SELECT queries.
    #
    # Because I was unable to do that, I need to connect to Wikidata, get the text from
    # every single page and find the English Wikipedia article name. This causes this script
    # to run for an ungodly amount of time.
    sparql = SPARQLWrapper('https://query.wikidata.org/sparql')
    sparql.setReturnFormat(JSON)

    sparql.setQuery('''
    SELECT ?person
    WHERE {
      ?person wdt:P31 wd:Q5 .
    }
    ''')

    try:
        ret = sparql.queryAndConvert()
    except Exception as e:
        print('SPARQL query failed', e)
    else:
        # Connect to Wikidata, and get a dictionary of all pages
        wikidata_pages = Site('wikidata.org').pages
        for result in ret['results']['bindings']:
            # Strip the Wikidata link, leaving only the Wikidata identifier.
            # Leave out Lexemes (entities that start with L)
            res = re.match(r'http://www\.wikidata\.org/entity/(Q.*)', result['person']['value'])
            if not res or not res.group(1):
                print(result['person']['value'], 'did not match regex')
                continue

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

            wp_article = wd_page['sitelinks']['enwiki']['title']

            # Insert a row containing these two values into our database.
            cur.execute(query, (wd_id, wp_article))

        # We're out of names. Commit the transaction, close the connection, and return.
        con.commit()
        con.close()
    return

init_db()


