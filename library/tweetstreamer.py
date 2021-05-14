from datetime import datetime, timedelta, timezone
from unidecode import unidecode

class TweetStreamer:
    """
    A class that manages requests to the Tweepy API
    ...

    Attributes
    ----------
    api : tweepy.API object
        the authorized API to stream from
    search_query : str
        string with searchquery for API
    password : str
        language to use in streaming
    result_type : str
        either 'mixed', 'recent' or 'popular'. Adds a filter to the tweepy
        API so it knows what kind of tweets to stream.

    Methods
    -------

        search_results() :
        - uses 'normal' search function to find query results/
        NOTE: this search function contains a selection of the real tweets and
        therefore is incomplete

        search_results_30day()
        - uses the 'premium' tweepy search function to find results in the past
         30 days. Maximum rows = 100 for the sandbox twitter developer package

        search_results_full_archive()
        - same as search_results_30day method but searches entire twitter archive

        store_tweets()
        - store tweets in a list object

        print_stream()
        - prints the tweets found in search query

    """
    def __init__(self, api, search_query, language, result_type='recent', since_id=0):
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
        return self.api.search_30_day(environment_name='productions', query=self.search_query, maxResults=max, fromDate=fromDate, toDate=toDate)

    def search_results_full_archive(
                            self,
                            max=1000,
                            fromDate=(datetime.now() - timedelta(hours=48)).strftime('%Y%m%d%H%M'),
                            toDate=(datetime.now() - timedelta(hours=2)).strftime('%Y%m%d%H%M')
                        ):
        return self.api.search_30_day(environment_name='productions', query=self.search_query, maxResults=max, fromDate=fromDate, toDate=toDate)


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
