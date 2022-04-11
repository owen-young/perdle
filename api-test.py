from pywikiapi import wikipedia
import requests
import json
from datetime import datetime
from contextlib import redirect_stdout

# Connect to English Wikipedia
site = wikipedia('en')

def test():
    # Search for all backlinks to Tom Brady
    for p in site.query(bltitle='Tom_Brady', list=['backlinks'], blnamespace=0):
         print(p)


    # use another API to get the number of backlinks
    response = requests.get("http://linkcount.toolforge.org/api/?page=Tom_Brady&project=en.wikipedia.org")
    print(response.json()['wikilinks']['all'])

    # use REST API to get the number of page views
    current_date = datetime.today().strftime('%Y%m%d')

    req_headers = {
        'accept': 'application/json',
        'User-Agent': 'Perdle / v0 test'
    }
    response = requests.get("https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia.org/all-access/user/Tom_Brady/monthly/20220301/" + current_date, headers=req_headers)
    print(response.json())

def name_loop():
    # Template for Wikipedia pages that I think all Wikipedia pages for people include. This
    # may include animals as well, but I'll deal with that later.
    person_templates = ['Template:BirthDeathAge','Template:Birth_date','Template:Birth-date','Template:Birth_date_and_age',
                        'Template:Birth_date_and_age2','Template:Birth-date_and_age',
                        'Template:Birth_year_and_age','Template:Birth_based_on_age_as_of_date',
                        'Template:Birth_based_on_age_at_death','Template:Death_date','Template:Death-date',
                        'Template:Death_date_and_age','Template:Death-date_and_age',
                        'Template:Death_date_and_given_age','Template:Death_year_and_age']
    with open('names.txt', 'w') as f:
        # Loop through the query. Only include pages in namespace 0 (Main/Article).
        for res in site.query(titles=person_templates, prop=['transcludedin'],tinamespace=0):
            # Loop through each template and all the pages within it.
            for template in res.pages:
                # Loop through each page transcluded in this template. The API only handles one
                # title at a time, so most of the time, `template' will not contain an array of
                # pages (attribute `transcludedin')
                if hasattr(template, 'transcludedin'):
                    # Loop through the transcluded list, which has all pages that contain
                    # this template. Put them into a file for testing.
                    for page in template.transcludedin:
                        with redirect_stdout(f):
                            print(page.title)

name_loop()
