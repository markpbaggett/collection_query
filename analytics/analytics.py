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
        """..."""
        request = {
            "reportRequests": [
                {
                    "viewId": self.view_id,
                    "dateRanges": [
                        {"startDate": start_date, "endDate": end_date}
                    ],
                    "metrics": [{"expression": "ga:pageviews"}],
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
        for view in current_set['reports'][0]['data']['rows']:
            self.results.append(view)
        if 'nextPageToken' in current_set['reports'][0]:
            new_request = self.find_pages(token=current_set['reports'][0]['nextPageToken'])
            return self.process_pages(new_request)
        else:
            return


class AnalyticsInterpretter:
    def __init__(self, data):
        self.original_data = self.__sort_traffic_sources(data)
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


if __name__ == "__main__":
    import yaml
    collections = yaml.safe_load(open('config.yml', 'r'))['collections']
    connection = AnalyticsConnection(
        credentials="connection.json",
        view_id="118513499",
    )
    all_sources = {}
    for collection in collections:
        page = collection
        connection.process_pages(page=page, start_date='365daysago', end_date='today',)
        results = connection.results
        for result in results:
            x = {
                'source': result['dimensions'][1],
                'views': int(result['metrics'][0]['values'][0]),
                "actual_source": result['dimensions'][2]
            }
            if x['actual_source'] not in all_sources:
                all_sources[x['actual_source']] = x['views']
            else:
                all_sources[x['actual_source']] += x['views']
    print(AnalyticsInterpretter(all_sources).original_data)
