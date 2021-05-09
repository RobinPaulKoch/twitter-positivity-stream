import pandas as pd
import numpy as np
import tweepy
from unidecode import unidecode



def make_unicode(input):
    if type(input) != unicode:
        input =  input.decode('utf-8')
    return input


#Authorization part
# Keys and tokens
consumer_token = '6S01QCbapFtkIkac6nM00A9QM'
consumer_secret = 'XYuxZ1e455s8q5WOBB7OLhyrmL8hz3Pb3v82EHRGoLSQhDq7HE'
access_token = '1389993571433033731-9tZ7q2RO1QKqphvgIyxyiKiFO4Old4'
access_token_secret = '57A2mCH2y4d2vQf9c6PAX985ZyzglnU5iGNHR2hzJuHzX'

# Authorization
auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

try:
    api.verify_credentials()
    print("Authentication OK")
except:
    print("Error during authentication")


class TweetStream:
    def __init__(self, search_query, language, result_type):
        self.search_query = search_query
        self.language = language
        self.result_type = result_type

    def store_tweets(self):
        l = []
        print(self.search_query)
        for tweet in api.search(q=self.search_query, lang=self.language, result_type = self.result_type):
            tweet_text = unidecode(tweet.text)
            l.append(tweet_text)
        return l

    def print_stream(self, max):
        for tweet in api.search(q=self.search_query, lang=self.language, result_type = self.result_type, rpp=max):
            tweet_text = unidecode(tweet.text)
            print(f"tweet: {tweet_text}")

tweetstream = TweetStream('biertje', 'nl', 'recent')
tweets = tweetstream.store_tweets()
print(tweets)

#
# list = []
# for tweet in api.search(q="biertje", lang="nl", result_type = 'recent'):
#     tweet_text = unidecode(tweet.text)
#     # print(f"tweet: {tweet_text}")
#     list.append(tweet_text)

    # print(f"{tweet.user.nam}:{tweet.text}")


# for tweet in tweepy.Cursor(api.search, q='meow').items(10):
#     with open(tweet.text, "w", encoding="utf-8") as f:
#         print(f)

# try:
#     redirect_url = auth.get_authorization_url()
# except tweepy.TweepError:
#     print('Error! Failed to get request token.')
#
# session.set('request_token', auth.request_token['oauth_token'])
#
# verifier = raw_input('Verifier:')





#override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        print(status.text)

# myStreamListener = MyStreamListener()
# myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener())
