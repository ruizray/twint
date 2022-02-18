import re
import sqlite3
import sys
import time
import hashlib
import json
from datetime import datetime
from pprint import pprint
from urllib.parse import urlparse


def Conn(database):
    if database:
        print("[+] Inserting into Database: " + str(database))
        conn = init(database)
        if isinstance(conn, str):  # error
            print(conn)
            sys.exit(1)
    else:
        conn = ""

    return conn


def init(db):
    try:
        conn = sqlite3.connect(db)
        cursor = conn.cursor()

        # table_users = """
        #     CREATE TABLE IF NOT EXISTS
        #         users(
        #             id integer not null,
        #             id_str text not null,
        #             name text,
        #             username text not null,
        #             bio text,
        #             location text,
        #             url text,
        #             join_date text not null,
        #             join_time text not null,
        #             tweets integer,
        #             following integer,
        #             followers integer,
        #             likes integer,
        #             media integer,
        #             private integer not null,
        #             verified integer not null,
        #             profile_image_url text not null,
        #             background_image text,
        #             hex_dig  text not null,
        #             time_update integer not null,
        #             CONSTRAINT users_pk PRIMARY KEY (id, hex_dig)
        #         );
        #     """
        # cursor.execute(table_users)

        table_tweets = """
            CREATE TABLE IF NOT EXISTS twitter_tweets(id STRING UNIQUE, text TEXT,
            attachments dict,
            author_id TEXT,
            context_annotations list,
            conversation_id TEXT,
            created_at TIMESTAMP,
            entities dict,
            geo dict, 
            in_reply_to_user_id TEXT,
            lang TEXT, possibly_sensitive BOOLEAN,
            public_metrics dict,
            referenced_tweets list,
            reply_settings TEXT,
            source TEXT,
            withheld dict)
        """
        cursor.execute(table_tweets)

        # table_retweets = """
        #     CREATE TABLE IF NOT EXISTS
        #         retweets(
        #             user_id integer not null,
        #             username text not null,
        #             tweet_id integer not null,
        #             retweet_id integer not null,
        #             retweet_date integer,
        #             CONSTRAINT retweets_pk PRIMARY KEY(user_id, tweet_id),
        #             CONSTRAINT user_id_fk FOREIGN KEY(user_id) REFERENCES users(id),
        #             CONSTRAINT tweet_id_fk FOREIGN KEY(tweet_id) REFERENCES tweets(id)
        #         );
        # """
        # cursor.execute(table_retweets)

        # table_reply_to = """
        #     CREATE TABLE IF NOT EXISTS
        #         replies(
        #             tweet_id integer not null,
        #             user_id integer not null,
        #             username text not null,
        #             CONSTRAINT replies_pk PRIMARY KEY (user_id, tweet_id),
        #             CONSTRAINT tweet_id_fk FOREIGN KEY (tweet_id) REFERENCES tweets(id)
        #         );
        # """
        # cursor.execute(table_reply_to)

        # table_favorites =  """
        #     CREATE TABLE IF NOT EXISTS
        #         favorites(
        #             user_id integer not null,
        #             tweet_id integer not null,
        #             CONSTRAINT favorites_pk PRIMARY KEY (user_id, tweet_id),
        #             CONSTRAINT user_id_fk FOREIGN KEY (user_id) REFERENCES users(id),
        #             CONSTRAINT tweet_id_fk FOREIGN KEY (tweet_id) REFERENCES tweets(id)
        #         );
        # """
        # cursor.execute(table_favorites)

        # table_followers = """
        #     CREATE TABLE IF NOT EXISTS
        #         followers (
        #             id integer not null,
        #             follower_id integer not null,
        #             CONSTRAINT followers_pk PRIMARY KEY (id, follower_id),
        #             CONSTRAINT id_fk FOREIGN KEY(id) REFERENCES users(id),
        #             CONSTRAINT follower_id_fk FOREIGN KEY(follower_id) REFERENCES users(id)
        #         );
        # """
        # cursor.execute(table_followers)

        # table_following = """
        #     CREATE TABLE IF NOT EXISTS
        #         following (
        #             id integer not null,
        #             following_id integer not null,
        #             CONSTRAINT following_pk PRIMARY KEY (id, following_id),
        #             CONSTRAINT id_fk FOREIGN KEY(id) REFERENCES users(id),
        #             CONSTRAINT following_id_fk FOREIGN KEY(following_id) REFERENCES users(id)
        #         );
        # """
        # cursor.execute(table_following)

        # table_followers_names = """
        #     CREATE TABLE IF NOT EXISTS
        #         followers_names (
        #             user text not null,
        #             time_update integer not null,
        #             follower text not null,
        #             PRIMARY KEY (user, follower)
        #         );
        # """
        # cursor.execute(table_followers_names)

        # table_following_names = """
        #     CREATE TABLE IF NOT EXISTS
        #         following_names (
        #             user text not null,
        #             time_update integer not null,
        #             follows text not null,
        #             PRIMARY KEY (user, follows)
        #         );
        # """
        # cursor.execute(table_following_names)

        return conn
    except Exception as e:
        return str(e)


def fTable(Followers):
    if Followers:
        table = "followers_names"
    else:
        table = "following_names"

    return table


def uTable(Followers):
    if Followers:
        table = "followers"
    else:
        table = "following"

    return table


def follow(conn, Username, Followers, User):
    try:
        time_ms = round(time.time()*1000)
        cursor = conn.cursor()
        entry = (User, time_ms, Username,)
        table = fTable(Followers)
        query = f"INSERT INTO {table} VALUES(?,?,?)"
        cursor.execute(query, entry)
        conn.commit()
    except sqlite3.IntegrityError:
        pass


def get_hash_id(conn, id):
    cursor = conn.cursor()
    cursor.execute('SELECT hex_dig FROM users WHERE id = ? LIMIT 1', (id,))
    resultset = cursor.fetchall()
    return resultset[0][0] if resultset else -1


def user(conn, config, User):
    pprint(user)
    try:
        time_ms = round(time.time()*1000)
        cursor = conn.cursor()
        user = [int(User.id), User.id, User.name, User.username, User.bio, User.location, User.url, User.join_date, User.join_time, User.tweets,
                User.following, User.followers, User.likes, User.media_count, User.is_private, User.is_verified, User.avatar, User.background_image]

        hex_dig = hashlib.sha256(','.join(str(v)
                                 for v in user).encode()).hexdigest()
        entry = tuple(user) + (hex_dig, time_ms,)
        old_hash = get_hash_id(conn, User.id)

        if old_hash == -1 or old_hash != hex_dig:
            query = f"INSERT INTO users VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            cursor.execute(query, entry)
        else:
            pass

        if config.Followers or config.Following:
            table = uTable(config.Followers)
            query = f"INSERT INTO {table} VALUES(?,?)"
            cursor.execute(query, (config.User_id, int(User.id)))

        conn.commit()
    except sqlite3.IntegrityError:
        pass


def media(conn, created_at,user_id, username, media):
    entries = []
    media_keys = []
    for item in media:

        media_key = item['id']
        media_keys.append(media_key)
        height = item['sizes']['large']['h']
        width = item['sizes']['large']['w']
        aspect_ratio_l = None
        aspect_ratio_w = None
        content_type = None
        final_video_url = None
        duration_ms = None

        if 'video_info' in item:
            video_info = item['video_info']
            duration_ms = video_info.get('duration_millis')
            aspect_ratio_l = video_info['aspect_ratio'][0]
            aspect_ratio_w = video_info['aspect_ratio'][1]

            if item['type'] == 'animated_gif':
                content_type = video_info['variants'][0]['content_type']
                final_video_url = video_info['variants'][0]['url']

            else:
                for variant in video_info['variants']:

                    content_type = variant['content_type']
                    video_url = variant['url']
                    split_url = urlparse(video_url)
                    video_url = split_url.scheme+'://' + split_url.netloc+split_url.path
                    if video_url.endswith('.m3u8'):
                        pass

                    elif f"{width}x{height}" in video_url:
                        final_video_url = video_url

        entry = (
            media_key,
            user_id,
            username,
            created_at,
            item['type'],
            duration_ms,
            height,
            width,
            item.get('ext_alt_text'),
            item['media_url_https'],
            aspect_ratio_l,
            aspect_ratio_w,
            content_type,
            final_video_url,
            None,
            None
        )
        entries.append(entry)

    cursor = conn.cursor()
    cursor.executemany(
        """INSERT INTO twitter_media VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(media_key) DO UPDATE SET
            type = excluded.type,
            video_url = excluded.video_url,
            created_at = excluded.created_at""", entries)
    conn.commit()
    return media_keys


def tweets(conn: sqlite3.Connection, tweet, config):
    _dt = tweet['created_at']
    _dt = datetime.strptime(_dt, '%a %b %d %H:%M:%S %z %Y')
    created_at = _dt.strftime('%Y-%m-%d')
    referenced_tweets = []
    media_keys = []
    user_id = tweet['user_id']
    username = tweet['user_data']['screen_name']

    if tweet['retweeted'] or 'retweet_data' in tweet :
        pprint(tweet)
        quit()
    if tweet['is_quote_status']:
        try:
            referenced_tweets.append(
                {'type': 'quoted', 'id': tweet['quoted_status_id']})
        except:
            pass
    if 'media' in tweet['entities']:
        media_keys.extend(media(conn,created_at, user_id, username,
                           tweet['entities']['media']))
       
    if 'extended_entities' in tweet:
        print(tweet['extended_entities'].keys())
        del tweet['entities']['media']
        media_keys.extend(media(conn,created_at, user_id, username,
                           tweet['extended_entities']['media']))
    

    source = re.search('>(.*)<', tweet['source'])
    if source:
        source = source.group(1)

    mentioned_users  = [user['id'] for user in tweet['entities']['user_mentions']]
    del tweet['entities']['user_mentions']

    if 'in_reply_to_user_id' in tweet and tweet['in_reply_to_status_id']:
        referenced_tweets.append({'type': 'replied_to', 'id': tweet['in_reply_to_status_id']})
       
      
    _dt = tweet['created_at']
    _dt = datetime.strptime(_dt, '%a %b %d %H:%M:%S %z %Y')
    print(tweet['full_text'].replace('\n',''))
    entry = (
        tweet['id'],
        tweet['full_text'].replace('\n',''),
        json.dumps({}),
        tweet['user_id'],
        json.dumps({}),
        tweet['conversation_id'],
        created_at,
        json.dumps(tweet['entities']),
        json.dumps(tweet['geo']),
        json.dumps(tweet['place']),
        tweet['in_reply_to_user_id'],
        tweet['lang'],
        tweet.get('possibly_sensitive', False),
        tweet['retweet_count'],
        tweet['reply_count'],
        tweet['favorite_count'],
        tweet['quote_count'],
        json.dumps(referenced_tweets),
        json.dumps(media_keys),
        json.dumps(mentioned_users),
        None,
        source,
        None


    )
   
    cursor = conn.cursor()
    cursor.execute(
        f"""INSERT INTO twitter_tweets VALUES({','.join(['?']*len(entry))})
            ON CONFLICT(id) DO UPDATE SET
            created_at = excluded.created_at""", entry)
    conn.commit()
    return
    # try:
    #     time_ms = round(time.time()*1000)
    #     cursor = conn.cursor()
    #     entry = (Tweet.id,
    #              Tweet.id_str,
    #              Tweet.tweet,
    #              Tweet.lang,
    #              Tweet.conversation_id,
    #              Tweet.datetime,
    #              Tweet.datestamp,
    #              Tweet.timestamp,
    #              Tweet.timezone,
    #              Tweet.place,
    #              Tweet.replies_count,
    #              Tweet.likes_count,
    #              Tweet.retweets_count,
    #              Tweet.user_id,
    #              Tweet.user_id_str,
    #              Tweet.username,
    #              Tweet.name,
    #              Tweet.link,
    #              json.dumps(Tweet.mentions),
    #              json.dumps(Tweet.hashtags),
    #              json.dumps(Tweet.cashtags),
    #              json.dumps(Tweet.urls),
    #              json.dumps(Tweet.photos),
    #              Tweet.thumbnail,
    #              Tweet.quote_url,
    #              Tweet.video,
    #              Tweet.geo,
    #              Tweet.near,
    #              Tweet.source,
    #              time_ms,
    #              Tweet.translate,
    #              Tweet.trans_src,
    #              Tweet.trans_dest)

    # if config.Favorites:
    #     query = 'INSERT INTO favorites VALUES(?,?)'
    #     cursor.execute(query, (config.User_id, Tweet.id))

    # if Tweet.retweet:
    #     query = 'INSERT INTO retweets VALUES(?,?,?,?,?)'
    #     _d = datetime.timestamp(datetime.strptime(
    #         Tweet.retweet_date, "%Y-%m-%d %H:%M:%S"))
    #     cursor.execute(query, (int(Tweet.user_rt_id),
    #                    Tweet.user_rt, Tweet.id, int(Tweet.retweet_id), _d))

    # if Tweet.reply_to:
    #     for reply in Tweet.reply_to:
    #         query = 'INSERT INTO replies VALUES(?,?,?)'

    #         try:
    #             cursor.execute(query, (Tweet.id, int(
    #                 reply['user_id']), reply['username']))
    #         except:
    #             cursor.execute(query, (Tweet.id, int(
    #                 reply['id']), reply['screen_name']))

    # conn.commit()
    # except sqlite3.IntegrityError:
    #     pass
    # except sqlite3.InterfaceError:
    #     print(entry)
    #     pass
