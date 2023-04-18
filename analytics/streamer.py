from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import csv
import datetime


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

    def find_pages(self, token=None, start_date="45daysAgo", end_date="today"):
        request = {
            "reportRequests": [
                {
                    "viewId": self.view_id,
                    "dateRanges": [
                        {"startDate": start_date, "endDate": end_date}
                    ],
                    "metrics": [{"expression": "ga:uniquePageviews"}],
                    "dimensions": [{"name": "ga:pagePath"}],
                    "pageSize": 10000,
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

    def process_pages(self, initial_pages=None, start_date=None, end_date=None):
        if initial_pages is None and start_date is not None and end_date is not None:
            current_set = self.find_pages( start_date=start_date, end_date=end_date)
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
    # import yaml
    import calendar

    start_year = 2019
    start_month = 7
    end_year = 2022
    end_month = 7

    months = []
    for year in range(start_year, end_year + 1):
        start_month_range = start_month if year == start_year else 1
        end_month_range = end_month if year == end_year else 12
        for month in range(start_month_range, end_month_range + 1):
            days_in_month = calendar.monthrange(year, month)[1]
            start_date = f"{year}-{month:02d}-01"
            end_date = f"{year}-{month:02d}-{days_in_month:02d}"
            name = calendar.month_name[month][:3] + ' ' + str(year)
            months.append({"start": start_date, "end": end_date, "name": name})
    connection = AnalyticsConnection(
        credentials="connection.json",
        view_id="42472462",
    )
    for month in months:
        connection.process_pages(start_date=month['start'], end_date=month['end'])
        results = connection.results
        final_results = []
        for result in results:
            path = f"https://streamer.lib.utk.edu{result['dimensions'][0]}"
            matching_dict = next((d for d in final_results if d['path'] == path), None)
            if matching_dict:
                matching_dict['views'] += int(result['metrics'][0]['values'][0])
            else:
                final_results.append({'path': path, 'views': int(result['metrics'][0]['values'][0])})
        with open(f"months/{month['name']}.csv", 'w') as f:
            writer = csv.DictWriter(f, fieldnames=['path', 'views'])
            writer.writeheader()
            writer.writerows(final_results)




