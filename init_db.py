import sqlite3
import re
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

    # Create a SPARQL query to get all humans.
    sparql = SPARQLWrapper('https://query.wikidata.org/sparql')
    sparql.setReturnFormat(JSON)

    sparql.setQuery('''
    SELECT ?person
    WHERE {
      ?person wdt:P31 wd:Q5 .
    }
    ''')

    # Create a query to insert a row for each person in the SPARQL query.
    query = '''
            INSERT INTO popularity (wd_id, wp_article) VALUES (?, ?)
            '''

    # Because the JSON response for the SPARQL query asking the question, "Give me a list
    # of all people with an entry on English Wikipedia", similar to this query:
    # https://www.wikidata.org/wiki/Wikidata:SPARQL_query_service/queries/examples#Countries_that_have_sitelinks_to_en.wiki
    #
    # was too large for JSON or XML to parse, extra querying is required. This formatted
    # query is meant to get that information and put it in the popularity database.
    wiki_article_query = '''
    SELECT ?article ?articleName
    WHERE {{
      ?article schema:about wd:{} .
      ?article schema:isPartOf <https://en.wikipedia.org/>;
      schema:name ?articleName
    }}
    '''

    # Create a list of all Wikidata entries that don't have a page on English Wikipedia.
    no_english_list = []

    try:
        ret = sparql.queryAndConvert()
    except Exception as e:
        print('SPARQL query failed', e)
    else:
        for result in ret['results']['bindings']:
            # Strip the Wikidata link, leaving only the Wikidata identifier.
            # Leave out Lexemes (entities that start with L)
            res = re.match(r'http://www\.wikidata\.org/entity/(Q.*)', result['person']['value'])
            if not res or not res.group(1):
                # Move on
                continue

            wd_id = res.group(1)

            # Query Wikidata to get the Wikipedia article name.
            sparql.setQuery(wiki_article_query.format(wd_id))
            wiki_name_ret = sparql.queryAndConvert()

            # If we didn't find it, add it to a list of people that don't exist on English
            # Wikipedia. Maybe I'll use it later, who knows. This gets output to non-english-list.txt.
            if not wiki_name_ret['results']['bindings']:
                no_english_list.append(wd_id)
                continue

            # Get the article name from the JSON response.
            wp_article = wiki_name_ret['results']['bindings'][0]['articleName']['value']

            # Insert a row containing these two values into our database.
            cur.execute(query, (wd_id, wp_article))

        # We're out of names. Commit the transaction, close the connection, and return.
        con.commit()
        con.close()

    with open('non-english-list.txt', 'x') as f:
        for entry in no_english_list:
            f.write(f'{entry}\n')

    return

init_db()