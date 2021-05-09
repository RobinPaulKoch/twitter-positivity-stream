import pandas as pd
import numpy as np
import tweepy
from tweepy import StreamListener, Stream
from unidecode import unidecode
import json
import csv

tweepy.__version__

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


consumer_token = '6S01QCbapFtkIkac6nM00A9QM'
consumer_secret = 'XYuxZ1e455s8q5WOBB7OLhyrmL8hz3Pb3v82EHRGoLSQhDq7HE'
access_token = '1389993571433033731-9tZ7q2RO1QKqphvgIyxyiKiFO4Old4'
access_token_secret = '57A2mCH2y4d2vQf9c6PAX985ZyzglnU5iGNHR2hzJuHzX'

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
