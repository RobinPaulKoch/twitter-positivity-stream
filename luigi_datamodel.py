import luigi
import os

# for running: python -m luigi --module Luigi_datamodel DataPipeline --local-scheduler

class InsertNewTweets(luigi.Task):

    def run(self):
        os.system('python tweepystream.py')

class RankTweets(luigi.Task):

    def run(self):
        os.system('rank_tweets.py')

if __name__ == '__main__':
     luigi.build([InsertNewTweets(), RankTweets()], local_scheduler=True)
