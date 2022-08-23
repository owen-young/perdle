import sqlite3
import json
import re
from SPARQLWrapper import SPARQLWrapper, JSON
import aiohttp
import asyncio

async def init_db():
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

    # This formatted query is used to split up the people from c. 1500 - 1800, since sometimes
    # this query would fail.
    formatted_1500_1800_query = '''
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
    LIMIT 1000
    '''

    # Generate a list of queries for each part of the century, as outlined above.
    query_1500_1800_list = get_formatted_query_list(formatted_1500_1800_query, 1500, 1800, 100)

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
    LIMIT 1000
    '''

    # This query is used to split up people born between c. 1900 - 2000, because this query
    # altogether (for all 4000 people) consistently timed out.
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
    LIMIT 200
    '''

    # Generate a list of queries for each part of the century, as outlined above.
    query_1900s_list = get_formatted_query_list(formatted_1900s_query, 1900, 2000, 5)

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

    query_list = [before_1500_query, between_1800_1900_query,
                  after_2000_query] + query_1900s_list + query_1500_1800_list


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

    # Asynchronously perform each SPARQL query and INSERT the results into our
    # SQLite database.
    async with aiohttp.ClientSession() as session:
        # Create a task for every query.
        tasks = []

        for query in query_list:
            tasks.append(asyncio.ensure_future(wd_sparql_query(session, query)))

        query_results = await asyncio.gather(*tasks)
        for result in query_results:
            # Insert a row containing these tuples into our database.
            try:
                cur.executemany(insert_query, result)
            except sqlite3.IntegrityError:
                print('Duplicate found')

    con.commit()
    con.close()
    return

# Routine to perform the GET request on the SPARQL endpoint asynchronously.
async def wd_sparql_query(session, query):

    # Define the header and parameters for upcoming GET request.
    url = 'https://query.wikidata.org/sparql'
    wiki_params = {'format': 'json', 'query': query}

    # Perform the GET request for our query and loop through the results, returning
    # a tuple of all Wikidata entity names, Wikipedia article names, and number of
    # Wiki sitelinks.
    tuple_list = []

    async with session.get(url, params=wiki_params) as resp:
        try:
            query_results = await resp.json()
        except Exception as e:
            print(query)
            raise e
        for result in query_results['results']['bindings']:
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

    # Return the list of all (wd_id, wp_article, wd_sitelinks) tuples from this query.
    return tuple_list


# Return a list of queries from a formatted query, giving ranges of time, such
# as c. 1900 - 1905, ... c. 1995 - 2000. This only works, however, if the step
# is divisible by the amount of time being covered. Otherwise you won't get
# enough queries.
def get_formatted_query_list(formatted_query, start, end, step):

    # Make sure we got the correct input.
    if ((end - start) % step) != 0:
        raise RuntimeError('Invalid year range provided!')

    # Generate a list of queries for each part of the range provided.
    query_list = []

    # Year in the most recent query.
    last_year_in_query = start

    for i in range(start+step, end+1, step):
        query_list.append(formatted_query.format(last_year_in_query, i))
        last_year_in_query = i

    return query_list

asyncio.run(init_db())