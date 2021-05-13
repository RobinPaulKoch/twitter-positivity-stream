import pandas as pd
import numpy as np
import tweepy
import json
import csv

from threading import Thread
from datetime import datetime, timedelta, timezone
from textblob import TextBlob
from tweepy import StreamListener, Stream
from unidecode import unidecode

class TweetStreamer:
    def __init__(self, api, search_query, language, result_type, since_id=0):
        self.api = api
        self.search_query = search_query
        self.language = language
        self.result_type = result_type

    def search_results(self, count=100, since_id=0):
        return self.api.search(q=self.search_query, count=count, since_id=since_id)

    def search_results_30day(
                            self,
                            max=1000,
                            fromDate=(datetime.now() - timedelta(hours=48)).strftime('%Y%m%d%H%M'),
                            toDate=(datetime.now() - timedelta(hours=2)).strftime('%Y%m%d%H%M')
                        ):
        return self.api.search_30_day(environment_name='production', query=self.search_query, maxResults=max, fromDate=fromDate, toDate=toDate)

    def store_tweets(self):
        l = []
        for tweet in self.api.search(q=self.search_query, lang=self.language, result_type = self.result_type):
            tweet_text = unidecode(tweet.text)
            l.append(tweet_text)
        return l

    def print_stream(self, max):
        for tweet in self.api.search(q=self.search_query, lang=self.language, result_type = self.result_type, rpp=max):
            tweet_text = unidecode(tweet.text)
            print(f"tweet: {tweet_text}")
