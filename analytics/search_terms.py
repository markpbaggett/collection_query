from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import csv
import json


class AnalyticsConnection:
    def __init__(
            self,
            credentials,
            view_id,
            scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    ):
        self.credentials_location = credentials
        self.view_id = view_id
        self.scopes = scopes
        self.results = []
        self.connection = self.__connect()

    def __connect(self):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.credentials_location, self.scopes
        )
        return build("analyticsreporting", "v4", credentials=credentials)

    def find_pages(self, page=None, token=None, start_date="45daysAgo", end_date="today"):
        request = {
            "reportRequests": [
                {
                    "viewId": self.view_id,
                    "dateRanges": [
                        {"startDate": start_date, "endDate": end_date}
                    ],
                    "metrics": [{"expression": "ga:uniquePageviews"}],
                    "dimensions": [{"name": "ga:pagePath"}],
                    "pageSize": 70000,
                    "dimensionFilterClauses": [
                        {
                            "filters": [
                                {
                                    "operator": "BEGINS_WITH",
                                    "dimensionName": "ga:pagePath",
                                    "expressions": [
                                        page
                                    ]
                                }
                            ]
                        }
                    ],
                }
            ]
        }
        if token is not None:
            request['reportRequests'][0]['pageToken'] = str(token)
        return (
            self.connection.reports()
            .batchGet(
                body=request
            )
            .execute()
        )

    def process_pages(self, page, initial_pages=None, start_date=None, end_date=None):
        if initial_pages is None and start_date is not None and end_date is not None:
            current_set = self.find_pages(page, start_date=start_date, end_date=end_date)
        elif initial_pages is None:
            current_set = self.find_pages()
        else:
            current_set = initial_pages
        try:
            for view in current_set['reports'][0]['data']['rows']:
                self.results.append(view)
        except KeyError:
            pass
        if 'nextPageToken' in current_set['reports'][0]:
            new_request = self.find_pages(token=current_set['reports'][0]['nextPageToken'])
            return self.process_pages(new_request)
        else:
            return


class SearchTerm:
    def __init__(self, url):
        self.url = url
        self.search_term = self.find_search_term()
        self.results = self.build_results()

    def find_search_term(self):
        return self.url.split('/')[4].split('?')[0].split('&f')[0]

    def find_type(self):
        try:
            if self.url.split('digital.lib.utk.edu/collections/islandora/search/')[1].startswith('utk_mods_'):
                return 'facet'
            else:
                return 'search_term'
        except IndexError:
            return 'browse_and_pagination'

    def find_collection_if_exists(self):
        if '&cp' in self.url:
            return self.url.split('&cp=')[1].split('&')[0]
        else:
            return None

    def find_solr_search_navigation(self):
        facets = []
        if '&islandora_solr_search_navigation=' in self.url:
            split_url = self.url.split('&f[')
            i = 0
            for field in split_url:
                if i!= 0:
                    facet_string = field.split('=')[1]
                    if facet_string != "":
                        facets.append(field.split('=')[1])
                i +=1
        return facets

    def build_results(self):
        return {
            'search_term': self.find_search_term(),
            'collection': self.find_collection_if_exists(),
            'facets': self.find_solr_search_navigation(),
            'full_string': [self.url],
            'type': self.find_type()
        }


class SearchTermSorter:
    def __init__(self, terms):
        self.__terms = terms

    def sort(self):
        return sorted(self.__terms.items(), key=lambda x:x[1]['values'], reverse=True)


if __name__ == "__main__":
    import yaml
    connection = AnalyticsConnection(
        credentials="connection.json",
        view_id="118513499",
    )
    page = "digital.lib.utk.edu/collections/islandora/search"
    connection.process_pages(page=page, start_date='365daysago', end_date='today',)
    results = connection.results
    search_terms = {}
    for result in results:
        if len(result['dimensions'][0].split('/')) >= 5:
            search_term = SearchTerm(result['dimensions'][0]).results
            if search_term['search_term'] != '' and search_term['type'] == 'search_term' and search_term['search_term'] not in search_terms:
                search_terms[search_term['search_term']] = {
                    'values': int(result['metrics'][0]['values'][0]),
                    'collections': [],
                    'facets': search_term['facets'],
                    'searches': search_term['full_string']
                }
                if search_term['collection'] is not None:
                    search_terms[search_term['search_term']]['collections'].append(search_term['collection'])
            elif search_term['search_term'] != '' and search_term['type'] == 'search_term':
                search_terms[search_term['search_term']]['values'] += int(result['metrics'][0]['values'][0])
                search_terms[search_term['search_term']]['searches'].append(search_term['full_string'][0])
                if search_term['collection'] is not None and search_term['collection'] not in search_terms[search_term['search_term']]['collections']:
                    search_terms[search_term['search_term']]['collections'].append(search_term['collection'])
                if len(search_term['facets']) > 0:
                    for facet in search_term['facets']:
                        if facet not in search_terms[search_term['search_term']]['facets']:
                            search_terms[search_term['search_term']]['facets'].append(facet)
        else:
            facet = result['dimensions'][0].split('=')[-1]
            if facet == '0':
                #print(result['dimensions'][0])
                pass
    final = SearchTermSorter(search_terms).sort()
    with open('datasets/search_terms/basic.csv', 'w') as basic:
        writer = csv.DictWriter(basic, fieldnames=['search_term', 'total'])
        writer.writeheader()
        for k, v in final:
            writer.writerow(
                {
                    'search_term': k,
                    'total': v['values']
                }
            )
    json_object = json.dumps(final, indent=4)
    with open("datasets/search_terms/full.json", "w") as outfile:
        outfile.write(json_object)
