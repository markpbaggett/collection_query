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


if __name__ == "__main__":
    import yaml
    connection = AnalyticsConnection(
        credentials="connection.json",
        view_id="118513499",
    )
    page = "digital.lib.utk.edu/collections/islandora/search"
    connection.process_pages(page=page, start_date='10daysago', end_date='today',)
    results = connection.results
    search_terms = {}
    for result in results:
        if len(result['dimensions'][0].split('/')) >= 5:
            search_term = result['dimensions'][0].split('/')[4].split('?')[0]
            if search_term != '' and search_term not in search_terms:
                search_terms[search_term] = int(result['metrics'][0]['values'][0])
            elif search_term != '':
                search_terms[search_term] += int(result['metrics'][0]['values'][0])
        else:
            facet = result['dimensions'][0].split('=')[-1]
            if facet == '0':
                print(result['dimensions'][0])
    #print(search_terms)

