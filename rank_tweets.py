from configurations import config
from library.database_connector_class import MySQLConnection
from library.dbfunctions import DBTrafficker

# Can later move the constants to the luigi file.
DBNAME = config.DBNAME
TBLNAME = config.TBLNAME
SEARCHQ = config.SEARCHQ
UTCTIME_DIFF = config.UTCTIME_DIFF #If you want to transfer the retrieved dates to "localtime"

if __name__ == "__main__":
    #Set up the MySQL connection:
    SQLconnection = MySQLConnection(DBNAME)
    SQLconnection.test_connect()
    db_trafficker = DBTrafficker(SQLconnection)

    # Conditional drop on the table
    sql = f"""DROP TABLE IF EXISTS {DBNAME}.ranked_tweets;"""
    SQLconnection.execute(sql)

    # Statement to get ranking of tweets based on their rankings in sentiment / smiley usage
    sql = f"""
        CREATE TABLE {DBNAME}.ranked_tweets AS

        	SELECT
        		*
        		,ROW_NUMBER() OVER(PARTITION BY twitter_query, within_hour ORDER BY emoji_count DESC) AS rank_emoji
        		,ROW_NUMBER() OVER(PARTITION BY twitter_query, within_hour ORDER BY sentiment DESC) AS rank_sentiment
        		,ROW_NUMBER() OVER(PARTITION BY twitter_query, within_hour ORDER BY subjectivity DESC) AS rank_subjectivity

        	FROM
        	(
        		SELECT
        			*
        			,date_format(created_at,'%H') as within_hour
        		FROM {DBNAME}.tweet_records ttr
        	) ttr

            WHERE created_at >= NOW() - INTERVAL 48 HOUR
            ORDER BY rank_emoji DESC, rank_sentiment DESC, rank_subjectivity DESC;
    """
    SQLconnection.execute(sql)
