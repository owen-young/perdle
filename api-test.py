from pywikiapi import wikipedia
from SPARQLWrapper import SPARQLWrapper, JSON
import requests
import json
from datetime import datetime
from contextlib import redirect_stdout

# Connect to English Wikipedia
site = wikipedia('en')

def test():
    # Search for all backlinks to Tom Brady
    count = 0
    for p in site.query(bltitle='Tom_Brady', list=['backlinks'], blnamespace=0):
         count += 1

    print("wikipedia API count:", count)

    # use another API to get the number of backlinks
    response = requests.get("http://linkcount.toolforge.org/api/?page=Tom_Brady&project=en.wikipedia.org")
    print(response.json()['wikilinks']['all'])

    # use REST API to get the number of page views
    current_date = datetime.today().strftime('%Y%m%d')

    req_headers = {
        'accept': 'application/json',
        'User-Agent': 'Perdle / owen.young0@protonmail.com'
    }
    response = requests.get("https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia.org/all-access/user/Tom_Brady/monthly/20220301/" + current_date, headers=req_headers)
    # print(response.json())

# Loop through all pages with Infobox and see if they're people using Dbpedia.
def name_loop():
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    sparql.setReturnFormat(JSON)
    with open('names.txt', 'x') as f:
        # Loop through the query. Only include pages in namespace 0 (Main/Article).
        for res in site.query(titles=['Template:Infobox'], prop=['transcludedin'],tinamespace=0):
            # Loop through each template.
            for template in res.pages:
                # Loop through each page with an Infobox and find out which one are people.
                for page in template.transcludedin:
                    # Create a query to select this page only if it is a Person via SPARQL endpoint.
                    sparql.setQuery("""
                    SELECT *
                    WHERE
                    {{
                    ?page a    dbo:Person ;
                    rdfs:label "{title}"@en
                    }}""".format(title=page.title))
                    try:
                        ret = sparql.queryAndConvert()
                        # If we got any results from the query, add this name to our file, because it is a person.
                        if ret["results"]["bindings"]:
                            with redirect_stdout(f):
                                print(page.title)
                    except Exception as e:
                        print(e)

test()



