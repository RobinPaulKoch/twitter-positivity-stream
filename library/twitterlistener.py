import pandas as pd
import numpy as np
import tweepy
from config import db_user, db_password, api_key, api_secret_key, access_token, access_token_secret
from tweepy import StreamListener, Stream
from unidecode import unidecode
import json
import csv

class StdOutListener(StreamListener):
    def on_status(self, status):
        # Filtering English language tweets from users with more than 500 followers
        if (status.lang == "en") & (status.user.followers_count >= 500):
            # Creating this formatting so when exported to csv the tweet stays on one line
            tweet_text = "'" + status.text.replace('\n', ' ') + "'"
            csvw.writerow([status.id,
                           status.user.screen_name,
                           # created_at is a datetime object, converting to just grab the month/day/year
                           status.created_at.strftime('%m/%d/%y'),
                           status.user.followers_count,
                           tweet_text])
            return True

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_error disconnects the stream
            return False

if __name__ == "__main__":

    consumer_token = api_key
    consumer_secret = api_secret_key
    access_token = access_token
    access_token_secret = access_token_secret

    # Authorization
    auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    l= StdOutListener()
    stream = Stream(auth, l)

    csvw = csv.writer(open("blank.csv", "a"))
    csvw.writerow(['twitter_id', 'name', 'created_at',
                   'followers_count', 'text'])
    stream.filter(track=['bierte'], is_async=True)

    stream.disconnect()
