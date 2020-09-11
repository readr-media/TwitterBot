import psycopg2
from psycopg2.extras import RealDictCursor

from configs.db_configs import DB_USER, DB_PASS, DB_IP, DB_PORT

connection = psycopg2.connect(host=DB_IP,
                              port=DB_PORT,
                              user=DB_USER,
                              password=DB_PASS,
                              database='twitter')
cur = connection.cursor(cursor_factory=RealDictCursor)


def execute_sql(sql: str, vars_):
    try:
        cur.execute(sql, vars_)
        # cur.execute(sql)
        connection.commit()
        print("OK")
    except Exception as e:
        print(sql)
        print(e)
        connection.rollback()


def insert_search_meta(item: dict):
    insert_sql = f"""
    INSERT INTO search_meta (completed_in, max_id, next_results, query, count, since_id)
    VALUES ({item['completed_in']}, {item['max_id']}, {item['next_results']}, {item['query']}, {item['count']}, {item['since_id']})
    """
    cur.execute(insert_sql)


def insert_user(item: dict):
    insert_sql = f"""
    INSERT INTO users (
    id, name, screen_name, location, description, url, protected, followers_count, friends_count, listed_count, created_at, favourites_count, geo_enabled, verified, statuses_count, contributors_enabled, is_translator, is_translation_enabled, profile_background_color, profile_background_image_url, profile_background_image_url_https, profile_background_tile, profile_image_url, profile_image_url_https, profile_link_color, profile_sidebar_border_color, profile_sidebar_fill_color, profile_text_color, profile_use_background_image, has_extended_profile, default_profile, default_profile_image, following, follow_request_sent, notifications, translator_type
    )
    VALUES (
        {item['id']},
        '{str(item['name'])}',
        '{str(item['screen_name'])}',
        '{str(item['location'])}',
        '{str(item.get('description', ''))}',
        '{str(item['url'])}',
        {item['protected']},
        {item['followers_count']},
        {item['friends_count']},
        {item['listed_count']},
        '{str(item['created_at'])}',
        {item['favourites_count']},
        {item['geo_enabled']},
        {item['verified']},
        {item['statuses_count']},
        {item['contributors_enabled']},
        {item['is_translator']},
        {item['is_translation_enabled']},
        '{str(item['profile_background_color'])}',
        '{str(item['profile_background_image_url'])}',
        '{str(item['profile_background_image_url_https'])}',
        {item['profile_background_tile']},
        '{str(item['profile_image_url'])}',
        '{str(item['profile_image_url_https'])}',
        '{str(item['profile_link_color'])}',
        '{str(item['profile_sidebar_border_color'])}',
        '{str(item['profile_sidebar_fill_color'])}',
        '{str(item['profile_text_color'])}',
        {item['profile_use_background_image']},
        {item['has_extended_profile']},
        {item['default_profile']},
        {item['default_profile_image']},
        {item['following']},
        {item['follow_request_sent']},
        {item['notifications']},
        '{str(item['translator_type'])}'
    )
    """
    try:
        cur.execute(insert_sql)
    except:
        connection.rollback()


def insert_table(table_name: str, item: dict):
    if isinstance(item["user"], dict):
        insert_user(item["user"])
        item["user"] = item["user"]["id"]

    insert_sql = f"""
    INSERT INTO {table_name} (created_at,id,text,truncated,source,in_reply_to_status_id,in_reply_to_user_id,in_reply_to_screen_name,users,is_quote_status,retweet_count,favorite_count,favorited,retweeted,lang)
    VALUES (
    %(created_at)s,
    %(id)s,
    %(text)s,
    %(truncated)s,
    %(source)s,
    %(in_reply_to_status_id)s,
    %(in_reply_to_user_id)s,
    %(in_reply_to_screen_name)s,
    %(user)s,
    %(is_quote_status)s,
    %(retweet_count)s,
    %(favorite_count)s,
    %(favorited)s,
    %(retweeted)s,
    %(lang)s
        )
    ON CONFLICT (id)
DO UPDATE
SET 
created_at=%(created_at)s,
text=%(text)s,
truncated=%(truncated)s,
source=%(source)s,
in_reply_to_status_id=%(in_reply_to_status_id)s,
in_reply_to_user_id=%(in_reply_to_user_id)s,
in_reply_to_screen_name=%(in_reply_to_screen_name)s,
users=%(user)s,
is_quote_status=%(is_quote_status)s,
retweet_count=%(retweet_count)s,
favorite_count=%(favorite_count)s,
favorited=%(favorited)s,
retweeted=%(retweeted)s,
lang=%(lang)s
    
    """
    execute_sql(insert_sql, item)


def create_table(table_name: str):
    creation_syntax = f"""
        create table {table_name}
    (
        created_at              varchar(50),
        id                      bigint not null
        constraint {table_name}_pk
            primary key,
        text                    text,
        truncated               boolean,
        entities                json,
        metadata                json,
        source                  varchar(50),
        in_reply_to_status_id   bigint,
        in_reply_to_user_id     bigint,
        in_reply_to_screen_name varchar(50),
        users                   bigint
            constraint {table_name}_users_fkey
                references users,
        coordinates             json,
        place                   json,
        is_quote_status         boolean,
        retweet_count           bigint,
        favorite_count          bigint,
        favorited               boolean,
        retweeted               boolean,
        possibly_sensitive      boolean,
        lang                    varchar(50)
    );"""
    cur.execute(creation_syntax)
    connection.commit()
