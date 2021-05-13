import pandas as pd
import numpy as np
import tweepy
import json
import csv
import textblob
import time
import re
import regex
import concurrent.futures

from configurations import config
from threading import Thread
from keys import db_user, db_password, api_key, api_secret_key, access_token, access_token_secret
from datetime import datetime, timedelta
from textblob import TextBlob
from library.database_connector_class import MySQLConnection
from library.dbfunctions import DBTrafficker
from library.tweetstreamer import TweetStreamer
from tweepy import StreamListener, Stream
from unidecode import unidecode

# Constants. Can later be distributed in seperate containers for scaling with Docker + Swarm of Kubernetes
DBNAME = config.DBNAME
TBLNAME = config.TBLNAME
SEARCHQ = config.SEARCHQ
UTCTIME_DIFF = config.UTCTIME_DIFF #If you want to transfer the retrieved dates to "localtime"
MAXTHREADS = 4
MAXROWS = 100 # This is the current bottleneck: the 30day search feature of the Tweepy API only handles 100 maxresult...

# Functions
def stream_data(twitter_search, endpoint, maxrows=100, multithread=False, maxthreads=4, **kwargs):

    if endpoint:

        #First recalculate time to UTC+0 time
        current_time_utc = datetime.now() - timedelta(hours=UTCTIME_DIFF, minutes=1)
        endpoint_utc = (endpoint - timedelta(hours=UTCTIME_DIFF, minutes=2))

        if multithread == False:
            # Singlethreaded
            fromDate = endpoint_utc.strftime('%Y%m%d%H%M')
            results = twitter_search.search_results_30day(max=maxrows, fromDate=fromDate)
            json_data = [r._json for r in results]
            df = pd.json_normalize(json_data)


        elif multithread == True:
            # Multithread the API call
            df = pd.DataFrame()
            futures = []
            data_list = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=maxthreads) as executor:
                delta = current_time_utc - endpoint_utc
                workloadfrac = delta/3
                for i in range(maxthreads):
                    time.sleep(1)
                    fromDate = (current_time_utc - (i+1)*workloadfrac).strftime('%Y%m%d%H%M')
                    toDate = (current_time_utc - i*workloadfrac).strftime('%Y%m%d%H%M')
                    futures.append(
                        executor.submit(twitter_search.search_results_30day, max=maxrows, fromDate=fromDate, toDate = toDate)
                    )
                for future in concurrent.futures.as_completed(futures):
                    json_data = [r._json for r in future.result()]
                    df = pd.concat([df, pd.json_normalize(json_data)], ignore_index=True)

    elif not creation_endpoint:
        results = twitter_search.search_results_30day(max=maxrows)
        json_data = [r._json for r in results]
        df = pd.json_normalize(json_data)

    return df


def make_unicode(input):
    if type(input) != unicode:
        input =  input.decode('utf-8')
    return input

def test_tweepy_connection(api):
    try:
        api.verify_credentials()
        print("Authentication OK")
    except:
        print("Error during authentication")

def retrieve_current_time():
    # datetime object containing current date and time
    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
    print("date and time =", dt_string)
    return dt_string

def count_emojis(s):
    emoji_pattern = re.compile('[\U0001F300-\U0001F64F]')
    emojis = emoji_pattern.findall(s)
    return len(emojis)

if __name__ == "__main__":

    #Get time of deployment for inserts into the database
    current_time = retrieve_current_time()

    #Set up the MySQL connection:
    SQLconnection = MySQLConnection(DBNAME)
    SQLconnection.test_connect()

    db_trafficker = DBTrafficker(SQLconnection)


    #Check if database with table already exists
    result = db_trafficker.check_tbl_exists(TBLNAME)

    if result == False:
        db_trafficker.create_dbtbl(TBLNAME)
        id_endpoint = 0
        created_endpoint = None
    else:
    #Fetch the latest timestamp and record ID from the database
        r = db_trafficker.fetch_times_db(TBLNAME)
        id_endpoint = r[0][0]
        creation_endpoint = r[0][1]
        print(f"latest query ID = {id_endpoint} with creation date {creation_endpoint}")


    #Authorization part for Twitter
    # Keys and tokens -> Set up your own config.py file with the tokens and keys!
    consumer_token = api_key
    consumer_secret = api_secret_key
    access_token = access_token
    access_token_secret = access_token_secret

    # Authorization
    auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)

    # Make streamer object
    twitter_search = TweetStreamer(api, SEARCHQ, 'en', 'recent')

    # Get Data from object
    df = stream_data(twitter_search, creation_endpoint, maxrows=MAXROWS, multithread=False)

    #Set up pd.dataframe with rows to insert into the DB
    df['created_at'] = pd.to_datetime(df['created_at']) + timedelta(hours=UTCTIME_DIFF)
    df['created_at'] = df['created_at'].dt.strftime("%Y-%m-%d %H:%M:%S")

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

    # Try using the db_trafficker class to insert into the database
    try:
        db_trafficker.insert_into_dbtbl(TBLNAME, rows)
        print(f"Insertion into the database completed!")
    except:
        raise Exception(f"Something went wrong with the insertion into the database")
