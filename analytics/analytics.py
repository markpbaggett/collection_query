from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build


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

    def find_pages(self, page, token=None, start_date="45daysAgo", end_date="today"):
        request = {
            "reportRequests": [
                {
                    "viewId": self.view_id,
                    "dateRanges": [
                        {"startDate": start_date, "endDate": end_date}
                    ],
                    "metrics": [{"expression": "ga:uniquePageviews"}],
                    "dimensions": [{"name": "ga:pagePath"}, {"name": "ga:fullReferrer"}, {"name": "ga:source"}, {"name": "ga:pageTitle"}],
                    "pageSize": 10000,
                    "dimensionFilterClauses": [
                        {
                            "filters": [
                                {
                                    "operator": "EXACT",
                                    "dimensionName": "ga:landingPagePath",
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


class AnalyticsInterpretter:
    def __init__(self, data):
        self.original_data = self.__sort_traffic_sources(self.__combine_similar_sources(data))
        self.total_views = self.__get_total_views(data)
        self.data_as_percentages = self.__as_percentages()

    @staticmethod
    def __get_total_views(data):
        total = 0
        for k, v in data.items():
            total += v
        return total

    def __as_percentages(self):
        x = {}
        for k, v in self.original_data.items():
            x[k] = '{:.1%}'.format(v/self.total_views)
        return x

    def count_percentages(self):
        total = 0
        for k, v in self.data_as_percentages.items():
            total = total + float(v.replace('%', ''))
        return total

    @staticmethod
    def __sort_traffic_sources(sortable):
        return dict(sorted(sortable.items(), key=lambda x: x[1], reverse=True))

    @staticmethod
    def __combine_similar_sources(data):
        sources_to_replace = {
            'search.google.com': 'google',
            't.co': 'twitter',
            'lm.facebook.com': 'facebook',
            'l.facebook.com': 'facebook',
            'us13.campaign-archive.com': 'mailchimp',
        }
        values_to_pop = []
        values_to_add = []
        for k, v in data.items():
            if k in sources_to_replace:
                values_to_pop.append(k)
                values_to_add.append({ sources_to_replace[k]: v})
        for value in values_to_pop:
            data.pop(value)
        for value in values_to_add:
            for k, v in value.items():
                if k in data:
                    data[k] += v
                else:
                    data[k] = v
        return data


if __name__ == "__main__":
    import yaml
    collections = yaml.safe_load(open('config.yml', 'r'))['collections']
    connection = AnalyticsConnection(
        credentials="connection.json",
        view_id="118513499",
    )
    all_sources = {}
    primo_collections = {}
    for collection in collections:
        page = collection
        connection.process_pages(page=page, start_date='365daysago', end_date='today',)
        results = connection.results
        for result in results:
            """
            Must ensure that the ga:pagePath is the same as what's in the config because ga:landingPagePaths do not
            ignore HTTP parameters like queries 
            (e.g. digital.lib.utk.edu/collections/islandora/object/collections:volvoices?page=16).
            """
            if result['dimensions'][0] == collection:
                x = {
                    'source': result['dimensions'][1],
                    'views': int(result['metrics'][0]['values'][0]),
                    "actual_source": result['dimensions'][2]
                }
                if x['actual_source'] not in all_sources:
                    all_sources[x['actual_source']] = x['views']
                else:
                    all_sources[x['actual_source']] += x['views']
                if "utk.primo.exlibrisgroup.com" in x['source']:
                    if collection not in primo_collections:
                        primo_collections[collection] = x['views']
                    else:
                        primo_collections[collection] += x['views']
    print(AnalyticsInterpretter(all_sources).original_data)
    print(AnalyticsInterpretter(all_sources).data_as_percentages)
    print(dict(sorted(primo_collections.items(), key=lambda x: x[1], reverse=True)))