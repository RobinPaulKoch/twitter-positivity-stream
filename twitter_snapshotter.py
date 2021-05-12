import pandas as pd
import numpy as np
import tweepy
import json
import csv
import textblob
import time
import re
import regex

from threading import Thread
from config import db_user, db_password, api_key, api_secret_key, access_token, access_token_secret
from datetime import datetime
from textblob import TextBlob
from library.database_connector_class import MySQLConnection
from library.dbtraffic import DBTrafficker
from library.tweetstreamer import TweetStreamer
from tweepy import StreamListener, Stream
from unidecode import unidecode

# Constants. Can later be distributed in seperate containers for scaling with Docker + Swarm of Kubernetes
DBNAME = 'twitterstream'
TBLNAME = 'tweet_records'
SEARCHQ = 'heineken'

# Functions
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
    time = retrieve_current_time()

    #Set up the MySQL connection:
    SQLconnection = MySQLConnection(DBNAME)
    SQLconnection.test_connect()

    db_trafficker = DBTrafficker(SQLconnection)

    #Check if database with table already exists
    result = db_trafficker.check_tbl_exists(TBLNAME)

    if result == False:
        db_trafficker.create_dbtbl(TBLNAME)
        id_endpoint = 0
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

    # twitter_search = TweetSnap('biertje', 'nl', 'recent')
    twitter_search = TweetStreamer(api, SEARCHQ, 'en', 'recent')
    results = twitter_search.search_results(count=100, since_id=id_endpoint)

    tweets = twitter_search.store_tweets()

    json_data = [r._json for r in results]
    df = pd.json_normalize(json_data)


    #Set up pd.dataframe with rows to insert into the DB
    df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime("%Y-%m-%d %H:%M:%S")
    rows = (
        # pd.concat([pd.Series([time for x in range(len(df.index))], name='deploy_time'), df[['id', 'created_at', 'text']]], axis = 1)
        pd.concat([
            pd.Series([SEARCHQ for x in range(len(df.index))], name='twitter_query'),
            pd.Series([time for x in range(len(df.index))], name='deploy_timestamp'),
            df[['id', 'created_at', 'text']]
            ], axis = 1)
        .assign(emoji_count = df['text'].apply(lambda text: count_emojis(text)))
        .assign(sentiment = df['text'].apply(lambda tweet: TextBlob(tweet).sentiment[0]))
        .assign(subjectivity = df['text'].apply(lambda tweet: TextBlob(tweet).sentiment[1]))
        .rename({'text': 'tweet'}, axis='columns')
    )

    # Try using the db_trafficker class to insert into the database
    try:
        db_trafficker.insert_into_dbtbl(TBLNAME, rows)
        print(f"Insertion into the database completed!")
    except:
        raise Exception(f"Something went wrong with the insertion into the database")
