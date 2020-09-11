import os.path
import pickle
from typing import List

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from configs.spreadsheet import SPREADSHEET_ID, RANGE_NAME
from pg import create_table, cur

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']


class TwitterDataRequest():
    def __init__(self, task_name, crawl_type='account', crawl_target='', is_start=False, is_ended=False):
        self.task_name = task_name
        self.type = crawl_type
        self.crawl_target = crawl_target
        self.is_start = is_start
        self.is_ended = is_ended

    def create_table_if_not_exist(self):
        cur.execute(f"select * from information_schema.tables where table_name='{self.task_name}'")

        if not bool(cur.rowcount):  # If table don't exist, Create.
            create_table(self.task_name)


def get_twitter_task() -> List[TwitterDataRequest]:
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'configs/cred.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    values = result.get('values', [])

    task_requests = []
    if not values:
        print('No data found.')
    else:
        print('CURRENT DATA:')
        for row in values:
            # Print columns A and E, which correspond to indices 0 and 4.
            if len(row) < 6:
                row.extend([None] * (6 - len(row)))
            print(row)

            task_requests.append(
                # possible for index out of range
                TwitterDataRequest(task_name=row[0], crawl_type=row[1], crawl_target=row[2], is_start=row[3],
                                   is_ended=row[5]))

    return task_requests  # returning a list


if __name__ == '__main__':
    get_twitter_task()
