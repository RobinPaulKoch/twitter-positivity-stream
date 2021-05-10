import pandas as pd
import numpy as np
import tweepy
import json
import csv
import textblob
import time

from config import db_user, db_password, api_key, api_secret_key, access_token, access_token_secret
from datetime import datetime
from textblob import TextBlob
from database_connector_class import MySQLConnection
from tweepy import StreamListener, Stream
from unidecode import unidecode

DBNAME = 'twitterstream'
TBLNAME = 'tweet_records'
SEARCHQ = 'heineken'
# pd.set_option("display.max_rows", None, "display.max_columns", None)

class TweetSnap:
    def __init__(self, search_query, language, result_type, since_id=0):
        self.search_query = search_query
        self.language = language
        self.result_type = result_type

    def search_results(self, count=100, since_id=0):
        return api.search(q=self.search_query, count=1000, since_id=since_id)

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


def check_tbl_exists(SQLConnection, tbl):

    tbl = 'tweet_records'
    sql = f"""
        SELECT * FROM information_schema.tables
        WHERE table_name = '{tbl}';
    """
    result = SQLconnection.execute(sql, return_result=True)

    if result:
        return True
    else:
        return False


def create_dbtbl(SQLConnection, tbl):

    sql = f"""
        CREATE TABLE twitterstream.{tbl}(
            twitter_query VARCHAR(100),
        	deploy_timestamp TIMESTAMP,
            id BIGINT PRIMARY KEY,
            created_at TIMESTAMP,
            tweet VARCHAR(250),
            sentiment FLOAT,
            subjectivity FLOAT
        );
    """
    SQLconnection.execute(sql)

def fetch_times_db(SQLconnection, tbl):
    sql = f"""SELECT MAX(id), MAX(created_at) FROM twitterstream.{tbl};"""
    return SQLconnection.execute(sql, return_result=True)


def insert_into_dbtbl(SQLconnection, tbl, rows):

    SQLEngine = SQLconnection.connect_with_alchemy()
    engine_connection = SQLEngine.connect()

    #Insert the rows into the database
    rows.to_sql(tbl, con=engine_connection, if_exists='append', index=False)

    engine_connection.close()

    print(f"Insertion into the database completed!")


if __name__ == "__main__":

    #Get time of deployment for inserts into the database
    time = retrieve_current_time()

    #Set up the MySQL connection:
    SQLconnection = MySQLConnection(DBNAME)
    SQLconnection.test_connect()

    #Check if database with table already exists
    result = check_tbl_exists(SQLconnection, TBLNAME)

    if result == False:
        create_dbtbl(SQLconnection, TBLNAME)
        id_enpoint = 0
    else:
    #Fetch the latest timestamp and record ID from the database
        r = fetch_times_db(SQLconnection, TBLNAME)
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
    twitter_search = TweetSnap(SEARCHQ, 'en', 'recent')
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
        .assign(sentiment = df['text'].apply(lambda tweet: TextBlob(tweet).sentiment[0]))
        .assign(subjectivity = df['text'].apply(lambda tweet: TextBlob(tweet).sentiment[1]))
        .rename({'text': 'tweet'}, axis='columns')
    )

    # Call function to insert rows into the database
    insert_into_dbtbl(SQLconnection, TBLNAME, rows)
