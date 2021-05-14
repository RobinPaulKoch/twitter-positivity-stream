import luigi
import os
from datetime import datetime
from luigi.contrib.mysqldb import MySqlTarget
from keys import  db_user, db_password
from configurations import config

# for running: python -m luigi --module Luigi_datamodel DataPipeline --local-scheduler

class InsertNewTweets(luigi.Task):
    """
    This task is the start of the data pipeline where the data streams in using
    the Tweepy API. Some database connection parameters are given to find the
    MySQL target of the script
    """
    rundate = luigi.DateParameter(default=datetime.now().date())
    host = "localhost:3306"
    db = config.DBNAME
    target_table = config.TBLNAME
    user = db_user
    pw = db_password

    def run(self):
        os.system('tweepystream.py')

    def get_target(self):
        return MySqlTarget(host=self.host, database=self.db, user=self.user, password=self.pw, table=self.target_table,
                           update_id=str(self.rundate))

    def requires(self):
        return []

    def output(self):
        return self.get_target()


class RankTweets(luigi.Task):
    """
    This script takes the input of 'InsertNewTweets' and performs some data
    manipulation to get the highest ranking tweets based on subjectivity, polarity and
    emoji count of each hour.
    """

    def requires(self):
        return InsertNewTweets()

    def run(self):
        os.system('rank_tweets.py')


if __name__ == '__main__':
     luigi.build([RankTweets()], workers=1, local_scheduler=True)
