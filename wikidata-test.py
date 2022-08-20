from pywikiapi import wikipedia
from SPARQLWrapper import SPARQLWrapper, JSON
import requests
import json
from datetime import datetime
from contextlib import redirect_stdout

# Connect to English Wikipedia
site = wikipedia('en')

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
sparql.setReturnFormat(JSON)

# Get all people on Wikidata, but this query fails. :(

query = """
SELECT ?item
WHERE {
  ?item wdt:P31 wd:Q5 .
}
"""
with open('names.txt', 'x') as f:
    try:
        ret = sparql.queryAndConvert()
        for name in ret["results"]["bindings"]:
            with redirect_stdout(f):
                print(name)
    except Exception as e:
        print(e)

