import time
import re
import regex
import concurrent.futures

import pandas as pd
import tweepy
import json
import textblob

from configurations import config
from keys import db_user, db_password, api_key, api_secret_key, access_token, access_token_secret
from datetime import datetime, timedelta
from textblob import TextBlob
from library.database_connector_class import MySQLConnection
from library.dbfunctions import DBTrafficker
from library.tweetstreamer import TweetStreamer
from tweepy import StreamListener, Stream
from unidecode import unidecode

"""
 Constants. Can later be obtained as system inputs so that you can appoint them dynamically
 from a orchestration tool. See also luigi_datamodel.py
"""
DBNAME = config.DBNAME
TBLNAME = config.TBLNAME
SEARCHQ = config.SEARCHQ
UTCTIME_DIFF = config.UTCTIME_DIFF #If you want to transfer the retrieved dates to "localtime"
PREMIUM_SEARCH = config.PREMIUM_SEARCH
MULTITHREAD = False
MAXTHREADS = 6
MAXROWS = 2500

# Functions
def stream_premium_data(twitterstreamer, endpoint, maxrows=100, multithread=False, threads=4, **kwargs):
    """ Uses the Twitter API premium search function. Has both options for Multithreading
    and singlethreaded calls. For first run only Singlethreaded running is provided.
    Workload is divided by estimating the timedelta between last entry creation and current UTC time

    Parameters
    ----------
    twitterstreamer : TwitterStreamer object (see library -> tweetstreamer)
        twitterstreamer object to handle requests to the Tweepy API

    endpoint : datetime
        Latest creation datetime read in the Database

    maxrows : int
        Maximum amount of tweets you want to stream. Maximum = 100

    multhitread : boolean
        True if you want to multithread or False if not

    threads : int
        Amount of threads you want to use when multithreading the API requests

    """

    if endpoint:

        #First recalculate time to UTC+0 time
        current_time_utc = datetime.now() - timedelta(hours=UTCTIME_DIFF, minutes=1)
        endpoint_utc = (endpoint - timedelta(hours=UTCTIME_DIFF, minutes=2))

        if multithread == False:
            # Singlethreaded
            fromDate = endpoint_utc.strftime('%Y%m%d%H%M')
            results = twitterstreamer.search_results_30day(max=maxrows, fromDate=fromDate)
            json_data = [r._json for r in results]
            df = pd.json_normalize(json_data)


        elif multithread == True:
            # Multithread the API call
            df = pd.DataFrame()
            futures = []
            data_list = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
                delta = current_time_utc - endpoint_utc
                workloadfrac = delta/3
                for i in range(threads):
                    # time.sleep(1)
                    fromDate = (current_time_utc - (i+1)*workloadfrac).strftime('%Y%m%d%H%M')
                    toDate = (current_time_utc - i*workloadfrac).strftime('%Y%m%d%H%M')
                    futures.append(
                        executor.submit(twitterstreamer.search_results_30day, max=maxrows, fromDate=fromDate, toDate = toDate)
                    )
                for future in concurrent.futures.as_completed(futures):
                    json_data = [r._json for r in future.result()]
                    df = pd.concat([df, pd.json_normalize(json_data)], ignore_index=True)

    elif not creation_endpoint:
        results = twitterstreamer.search_results_30day(max=maxrows)
        json_data = [r._json for r in results]
        df = pd.json_normalize(json_data)

    return df

def stream_normal_data(twitterstreamer, count=100, id_endpoint=0):
    """ Uses the Twitter API normalsearch function. This function is not limited
    by a maximum amount of results but the database with tweets is selective and incomplete

    Parameters
    ----------
    twitterstreamer : TwitterStreamer object (see library -> tweetstreamer)
        twitterstreamer object to handle requests to the Tweepy API

    count: int
        Maximum amount of tweets you want to stream.

    id_endpoint: bigint
        latest id entry in the database. Used for determining the delta to be
        requested to later insert into the DB.
    """

    results = twitterstreamer.search_results(count=count, since_id=id_endpoint)
    json_data = [r._json for r in results]
    return pd.json_normalize(json_data)

def translate_tweet(text):
    """I recommend not using this translation function as the TextBlob API
        for translation does not like it when you make too many requests
        shortly after each other. -> time.sleep(2)"""
    try:
        translation = TextBlob(text).translate(to='eng')
        time.sleep(2)
    except:
        translation = text

    return translation

def process_data(df, id_endpoint=0):
    """processes the returned DF from the API search. Returns a Dataframe object called
        'rows' which can be directly inserted into the DB"""

    # Format the creation date for MySQL timestamp standards
    df['created_at'] = pd.to_datetime(df['created_at']) + timedelta(hours=UTCTIME_DIFF)
    df['created_at'] = df['created_at'].dt.strftime("%Y-%m-%d %H:%M:%S")

    #Translate tweets:
    # df['text'] = df['text'].apply(lambda tweet: translate_tweet(tweet))

    # Some pandas data pipeline to create the scores and to subset the incoming DF
    rows = (
        pd.concat([
            pd.Series([SEARCHQ for x in range(len(df.index))], name='twitter_query'),
            pd.Series([current_time for x in range(len(df.index))], name='deploy_timestamp'),
            df[['id', 'created_at', 'text']]
            ], axis = 1)
        .assign(emoji_count = df['text'].apply(lambda text: count_emojis(text)))
        .assign(sentiment = df['text'].apply(lambda tweet: TextBlob(tweet).sentiment[0]))
        .assign(subjectivity = df['text'].apply(lambda tweet: TextBlob(tweet).sentiment[1]))
        .rename({'text': 'tweet'}, axis='columns')
        .loc[df['id'] > id_endpoint]
    )

    return rows


def make_unicode(input):
    """Resolves some issues when printing tweets utf-8 format"""
    if type(input) != unicode:
        input =  input.decode('utf-8')
    return input

def test_tweepy_connection(api):
    """tests connection with the API"""
    try:
        api.verify_credentials()
        print("Authentication OK")
    except:
        print("Error during authentication. Have you added a 'keys.py' file with \
        your API credentials??")

def retrieve_current_time():
    """retrieves current time and formats it in a readable string"""
    # datetime object containing current date and time
    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
    print("date and time =", dt_string)
    return dt_string

def count_emojis(s):
    """this function uses regular expressions to count the emojis in
    a tweet text"""
    emoji_pattern = re.compile('[\U0001F300-\U0001F64F]')
    emojis = emoji_pattern.findall(s)
    return len(emojis)


if __name__ == "__main__":

    #Get time of deployment for inserts into the database
    current_time = retrieve_current_time()

    #Set up the MySQL connection:
    SQLconnection = MySQLConnection(DBNAME)
    SQLconnection.test_connect()

    # Initialize a DBTrafficker class object
    db_trafficker = DBTrafficker(SQLconnection)

    #Check if database with table already exists
    result = db_trafficker.check_tbl_exists(TBLNAME)

    # Either the table exists already in the DB or not.
    # If DB.TBL does not exist then create one!
    if result == False:
        db_trafficker.create_dbtbl(TBLNAME)
        id_endpoint = 0
        creation_endpoint = None
    else:
    # If the DB.TBL exists: Fetch the latest timestamp and record ID from the DB
        r = db_trafficker.fetch_times_db(TBLNAME)
        id_endpoint = r[0][0]
        creation_endpoint = r[0][1]
        print(f"latest query ID = {id_endpoint} with creation date {creation_endpoint}")

    #Authorization part for Twitter
    # Keys and tokens -> Set up your own keys.py file with the tokens and keys!
    # You can use the keys_example.py file as a template
    consumer_token = api_key
    consumer_secret = api_secret_key
    access_token = access_token
    access_token_secret = access_token_secret

    # Authorization of the API
    auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # Create a Tweepy API object
    api = tweepy.API(auth)

    # Make streamer object
    twitterstreamer = TweetStreamer(api, SEARCHQ, 'recent')

    # Use either the Normal or Premium search functions of the tweepy API
    if PREMIUM_SEARCH == False:
        # Get Data from object
        df = stream_normal_data(twitterstreamer, count=MAXROWS, id_endpoint=id_endpoint)

    elif PREMIUM_SEARCH == True:
        # For premium search functions use:
        df = stream_premium_data(twitterstreamer, creation_endpoint, maxrows=MAXROWS, multithread=MULTITHREAD, threads=MAXTHREADS)

    #Dataprocessing steps
    rows = process_data(df, id_endpoint)


    # Try using the db_trafficker class to insert into the database
    try:
        db_trafficker.insert_into_dbtbl(TBLNAME, rows)
        print(f"Insertion into the database completed!")
    except:
        raise Exception(f"Something went wrong with the insertion into the database")
