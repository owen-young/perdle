from SPARQLWrapper import SPARQLWrapper, JSON
from contextlib import redirect_stdout

sparql = SPARQLWrapper('https://query.wikidata.org/sparql')
sparql.setReturnFormat(JSON)

# Get links to all people on Wikidata in names.txt.
# WARNING: This creates a file that is ~400mb large. 

sparql.setQuery("""
SELECT ?item
WHERE {
  ?item wdt:P31 wd:Q5 .
}
""")
with open('names.txt', 'x') as f:
    try:
        ret = sparql.queryAndConvert()
        for result in ret['results']['bindings']:
            with redirect_stdout(f):
                print(result['item']['value'])
    except Exception as e:
        print(e)

