from multiprocessing.pool import ThreadPool
import sqlite3
import re
from SPARQLWrapper import SPARQLWrapper, JSON
from concurrent.futures import ThreadPoolExecutor

def init_db():
    # Create our popularity database and populate it with People from Wikidata.

    con = sqlite3.connect("pop.db")
    cur = con.cursor()

    # Create the table if it does not already exist, which contains:
    #    wd_id (primary key): name on Wikidata, such as Q747.
    #    wp_article:          artice name on English Wikipedia.
    #    wd_sitelinks         number of sitelinks for a person's Wikidata page.
    #    wp_backlinks:        the number of backlinks on English Wikipedia for this person.
    #                         A backlink is a link to this Wikipedia page on another page.
    #    wp_avgviews:         Average monthly views starting July 2015 for this
    #                         English Wikipedia article.
    #    google_search_num:   Number of Google Search results for this English Wikipedia
    #                         article name.
    cur.execute('''
                CREATE TABLE IF NOT EXISTS popularity
                (wd_id PRIMARY KEY, wp_article, wd_sitelinks, wp_backlinks,
                wp_avgviews, google_search_num)
                ''')

    # Create a list of queries to perform that give us people with the most sitelinks
    # on Wikidata. Because of the sheer number of people on Wikidata (10,039,180), some
    # filtering needs to be done, because a vast, vast majority of them should not show
    # up in Perdle. This would not make the game fun. The weighting I've come up with here
    # (i.e., how many people from each time period) is subjective.

    # This query gets the 1000 people on Wikidata with the most sitelinks born before
    # 1500 and has a page on English Wikipedia. In my view, there are far fewer than 1000
    # people on this list considered "common knowledge," but I will get 1000 just in case.
    before_1500_query = '''
    SELECT DISTINCT ?person ?articleName ?sitelinks
    WHERE {
        ?person wdt:P31 wd:Q5;
                wdt:P569 ?birth;
        FILTER (?birth < "1500-01-01"^^xsd:dateTime) .
        ?person wikibase:sitelinks ?sitelinks .
        ?article schema:about ?person .
        ?article schema:isPartOf <https://en.wikipedia.org/>;
        schema:name ?articleName .
        SERVICE wikibase:label {
            bd:serviceParam wikibase:language "en"
        }
    }
    ORDER BY DESC(?sitelinks)
    LIMIT 1000
    '''

    # The queries going forward (from 1500 - present) need to be split up to avoid timeouts
    # and HTTP status code 500. This is especially true for the 20th century, where many many
    # people reside on Wikidata.

    # This query is the same as before_1500_query, but goes between c. 1500 - 1800 and
    # gets 4000 people.
    between_1500_1800_query = '''
    SELECT DISTINCT ?person ?articleName ?sitelinks
    WHERE {
        ?person wdt:P31 wd:Q5;
                wdt:P569 ?birth;
        FILTER (?birth >= "1500-01-01"^^xsd:dateTime && ?birth < "1800-01-01"^^xsd:dateTime) .
        ?person wikibase:sitelinks ?sitelinks .
        ?article schema:about ?person .
        ?article schema:isPartOf <https://en.wikipedia.org/>;
        schema:name ?articleName .
        SERVICE wikibase:label {
            bd:serviceParam wikibase:language "en"
        }
    }
    ORDER BY DESC(?sitelinks)
    LIMIT 4000
    '''

    # This query is the same as between_1500_1800_query, but it goes between c. 1800 - 1900 and
    # gets 3000 people.
    between_1800_1900_query = '''
    SELECT DISTINCT ?person ?articleName ?sitelinks
    WHERE {
        ?person wdt:P31 wd:Q5;
                wdt:P569 ?birth;
        FILTER (?birth >= "1800-01-01"^^xsd:dateTime && ?birth < "1900-01-01"^^xsd:dateTime) .
        ?person wikibase:sitelinks ?sitelinks .
        ?article schema:about ?person .
        ?article schema:isPartOf <https://en.wikipedia.org/>;
        schema:name ?articleName .
        SERVICE wikibase:label {
            bd:serviceParam wikibase:language "en"
        }
    }
    ORDER BY DESC(?sitelinks)
    LIMIT 3000
    '''

    # This query is the same as between_1800_1900_query, but it goes between c. 1900 - 2000 and
    # gets 3000 people total. A loop to create 4 different queries will use this formatted string
    # to generate queries for people between c. 1900 - 1925, c. 1925 - 1950, c. 1950 - 1975, and
    # c. 1975 - 2000.
    formatted_1900s_query = '''
    SELECT DISTINCT ?person ?articleName ?sitelinks
    WHERE {{
        ?person wdt:P31 wd:Q5;
                wdt:P569 ?birth;
        FILTER (?birth >= "{}-01-01"^^xsd:dateTime && ?birth < "{}-01-01"^^xsd:dateTime) .
        ?person wikibase:sitelinks ?sitelinks .
        ?article schema:about ?person .
        ?article schema:isPartOf <https://en.wikipedia.org/>;
        schema:name ?articleName .
        SERVICE wikibase:label {{
            bd:serviceParam wikibase:language "en"
        }}
    }}
    ORDER BY DESC(?sitelinks)
    LIMIT 750
    '''

    # Generate a list of queries for each quarter of the century, as outlined above.
    query_1900s_list = []

    # Year in the most recent query.
    last_year_in_query = 1900

    for i in range(1925, 2001, 25):
        query_1900s_list.append(formatted_1900s_query.format(last_year_in_query, i))
        last_year_in_query = i

    # This query is the same as between_1800_1900_query, but it goes from c. 2000 - present and
    # only gets 250 people. In my opinion, this category is too new to have a lot of entries.
    after_2000_query = '''
    SELECT DISTINCT ?person ?articleName ?sitelinks
    WHERE {
        ?person wdt:P31 wd:Q5;
                wdt:P569 ?birth;
        FILTER (?birth >= "2000-01-01"^^xsd:dateTime) .
        ?person wikibase:sitelinks ?sitelinks .
        ?article schema:about ?person .
        ?article schema:isPartOf <https://en.wikipedia.org/>;
        schema:name ?articleName .
        SERVICE wikibase:label {
            bd:serviceParam wikibase:language "en"
        }
    }
    ORDER BY DESC(?sitelinks)
    LIMIT 250
    '''

    query_list = [before_1500_query, between_1500_1800_query, between_1800_1900_query,
                  after_2000_query] + query_1900s_list


    # Create a query to insert a row for each person in the SPARQL query.
    #
    # NOTE: Some historical figures don't have exact birth dates. If these
    #       birth dates span across queries (for example Meera,
    #       https://www.wikidata.org/wiki/Q466330, has a birth date of
    #       1498 and 1504), then a sqlite3.IntegrityError is raised.
    #       I will elect to ignore these exceptions and move on, but the
    #       reason I don't just put an 'OR IGNORE' clause into my INSERT
    #       query is because I would like to output which one was a duplicate,
    #       just in case there is a bug in the database building code.
    #       Hopefully it works!
    insert_query = '''
                   INSERT INTO popularity (wd_id, wp_article, wd_sitelinks) VALUES (?, ?, ?)
                   '''

    # Start a thread to perform each of the queries, where each thread returns a list
    # of (wd_id, wp_article, wd_sitelinks) tuples to INSERT.
    with ThreadPoolExecutor() as executor:
        for result in executor.map(get_insert_list, query_list):
            # Insert a row containing these tuples into our database.
            try:
                cur.executemany(insert_query, result)
            except sqlite3.IntegrityError:
                print('Duplicate found')

    con.commit()
    con.close()

# Return a list of tuples to INSERT into the popularity table in pop.db.
def get_insert_list(query):

    # Create a SPARQLWrapper for performing SPARQL SELECT requests.
    sparql = SPARQLWrapper('https://query.wikidata.org/sparql')
    sparql.setReturnFormat(JSON)

    # Query the Wikidata SPARQL endpoint with the query given as input to this thread.
    sparql.setQuery(query)
    ret = sparql.queryAndConvert()

    # Define a list tuples to be INSERTed once this thread finishes.
    tuple_list = []

    for result in ret['results']['bindings']:
        # Extract the Wikidata entity ID from the link.
        res = re.match(r'http://www\.wikidata\.org/entity/(Q.*)', result['person']['value'])
        if not res or not res.group(1):
            # Move on
            continue

        wd_id = res.group(1)

        # Get the article name from the JSON response.
        wp_article = result['articleName']['value']

        # Get the number of sitelinks from the JSON response. If the response we got was not
        # an integer, something is wrong and this script must be changed.
        if result['sitelinks']['datatype'] != 'http://www.w3.org/2001/XMLSchema#integer':
            raise RuntimeError('Sitelinks data type is not an integer:',
                                result['sitelinks']['datatype'])

        wd_sitelinks = int(result['sitelinks']['value'])

        tuple_list.append((wd_id, wp_article, wd_sitelinks))

    # We are all out of results for this query. Return the tuple INSERT list.
    return tuple_list

init_db()