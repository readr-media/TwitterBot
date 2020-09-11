from twitter import Twitter, OAuth

from configs.spreadsheet import TWITTER_API_KEY, TWITTER_API_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET
from google_sheet import get_twitter_task
from pg import insert_table

task_requests = get_twitter_task()

t = Twitter(auth=OAuth(
    consumer_key=TWITTER_API_KEY,
    consumer_secret=TWITTER_API_SECRET_KEY,
    token=ACCESS_TOKEN,
    token_secret=ACCESS_TOKEN_SECRET
))
count = 500

for task in task_requests:
    if (task.is_start == 'y'):
        print(f"Starting task {task.task_name}")
        task.create_table_if_not_exist()

        if task.type == 'search':
            search_result = t.search.tweets(q=task.crawl_target, count=count)
            for item in search_result['statuses']:
                insert_table(task.task_name, item)

            # insert_search_meta(search_result['search_meta'])

        elif task.type == 'account':
            search_result = t.statuses.user_timeline(screen_name=task.crawl_target, count=count)
            for item in search_result:
                insert_table(task.task_name, item)

            # insert_search_meta(search_result['search_meta'])

    else:
        print(f"Task {task.task_name} is not starting")
