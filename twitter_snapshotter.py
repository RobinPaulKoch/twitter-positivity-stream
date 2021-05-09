import pandas as pd
import numpy as np
import tweepy
import json
import csv
import textblob
import time

from config import api_key, api_secret_key, access_token, access_token_secret
from datetime import datetime
from textblob import TextBlob
from database_connector_class import MySQLConnection
from tweepy import StreamListener, Stream
from unidecode import unidecode

DBNAME = 'twitterstream'
TBLNAME = 'tweet_records'
pd.set_option("display.max_rows", None, "display.max_columns", None)

class TweetSnap:
    def __init__(self, search_query, language, result_type):
        self.search_query = search_query
        self.language = language
        self.result_type = result_type

    def search_results(self):
        return api.search(q=self.search_query, count=100)


    def store_tweets(self):
        l = []
        for tweet in api.search(q=self.search_query, lang=self.language, result_type = self.result_type):
            tweet_text = unidecode(tweet.text)
            l.append(tweet_text)
        return l

    def print_stream(self, max):
        for tweet in api.search(q=self.search_query, lang=self.language, result_type = self.result_type, rpp=max):
            tweet_text = unidecode(tweet.text)
            print(f"tweet: {tweet_text}")

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

# Functions to facilitate data exchange with the database
def retrieve_current_time():
    # datetime object containing current date and time
    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
    print("date and time =", dt_string)
    return dt_string

def create_output_table(rows):
    sql = f"""
        SELECT * FROM information_schema.tables
        WHERE table_name = {tbl};
    """
    SQLconnection.execute(sql)


def insert_into_dbtbl(SQLconnection, tbl, rows):

    tbl = "tweet_records"
    SQLEngine = SQLconnection.connect_with_alchemy()
    engine_connection = SQLEngine.connect()


    sql = f"""
        SELECT * FROM information_schema.tables
        WHERE table_name = {tbl};
    """
    result = SQLconnection.execute(sql, return_result=True)

    if result is None:
        print(f"Table '{tbl}' does not exist yet! This run will be considered\
                to be the initial run and 48 hours of tweets will be stored\
                within the database!' ")

        sql = f"""
            CREATE TABLE twitterstream.tweet_records(
            	deploy_time TIMESTAMP,
                id BIGINT PRIMARY KEY,
                tweet VARCHAR(250),
                sentiment FLOAT
            );
        """
        SQLconnection.execute(sql)

    # TODO: Write function that ensures only the deltas are inserted into the database

    #Insert the rows into the database
    rows.to_sql(tbl, con=engine_connection, if_exists='append', index=False)

    engine_connection.close()

    print(f"Insertion into the database completed!")


#Get time of deployment for inserts into the database
time = retrieve_current_time()

#Set up the MySQL connection:
SQLconnection = MySQLConnection(DBNAME)
SQLconnection.test_connect()

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
twitter_search = TweetSnap('heineken', 'en', 'recent')
results = twitter_search.search_results()
tweets = twitter_search.store_tweets()

json_data = [r._json for r in results]
df = pd.json_normalize(json_data)

# Determine sentiment
rows = (
    # pd.concat([pd.Series([time for x in range(len(df.index))], name='deploy_time'), df[['id', 'created_at', 'text']]], axis = 1)
    pd.concat([pd.Series([time for x in range(len(df.index))], name='deploy_time'), df[['id', 'text']]], axis = 1)
    .assign(sentiment = df['text'].apply(lambda tweet: TextBlob(tweet).sentiment[0]))
    .rename({'text': 'tweet'}, axis='columns')
    # .assign(sentiment = df['text'].apply(lambda tweet: TextBlob(tweet).sentiment[1]))
)

# Call function to insert rows into the database
insert_into_dbtbl(SQLconnection, TBLNAME, rows)

# End of Script
