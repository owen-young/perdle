import aiohttp
import asyncio

async def main():
	query = '''
	SELECT ?person ?article ?sitelinks
	WHERE {
	  ?person wdt:P31 wd:Q5;
		  wdt:P569 ?birth;
	  FILTER (?birth >= "1970-01-01"^^xsd:dateTime && ?birth < "1980-01-01"^^xsd:dateTime) .
	  ?person wikibase:sitelinks ?sitelinks .
	  ?article schema:about ?person .
	  ?article schema:isPartOf <https://en.wikipedia.org/>.
	  SERVICE wikibase:label {
	    bd:serviceParam wikibase:language "en"
	  }
	}
	ORDER BY DESC(?sitelinks)
	LIMIT 400
	'''
	async with aiohttp.ClientSession() as session:
		tasks = []
		tasks.append(asyncio.ensure_future(wd_sparql_query(session, query)))
		
		query_result = await asyncio.gather(*tasks)


async def wd_sparql_query(session, query):
	url = 'https://query.wikidata.org/sparql'
	wiki_params = {'format': 'json', 'query': query}
	
	async with session.get(url, params=wiki_params) as resp:
		query_results = await resp.read()
		print(query_results)
	
	return

asyncio.run(main())
		
