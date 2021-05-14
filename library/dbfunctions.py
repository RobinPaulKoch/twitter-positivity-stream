import pandas as pd
import pymysql
import cryptography
from keys import db_user, db_password
from getpass import getpass
from mysql.connector import connect, Error
from sqlalchemy import create_engine

class DBTrafficker:
    """
    A class object that functions as the highway between the functional shell
    of the application and the database

    ...

    Attributes
    ----------
    MySQLConnection : a MySQLConnection class object
        Holds the authentication credentials and attributes to connect to the database

    Methods
    -------


    check_tbl_exists(self, tbl)
        Returns True if table exists within the DB. False otherwise

    create_dbtbl()
        For initial run creates the table instance on the chosen database

    fetch_times_db(self, tbl):
        Fetches the last record ID and creation time from database

    connect_with_alchemy(self):
        Creates an SQL engine object to efficiently insert rows to the database

    insert_into_dbtbl(self, tbl, rows):
        Uses the SQL engine to insert the given rows to the database

    """

    def __init__(self, SQLconnection):
        self.SQLconnection=SQLconnection


    # Functions to facilitate data exchange with the database

    def check_tbl_exists(self, tbl):
        """Returns true if tbl exists and False if not """

        sql = f"""
            SELECT * FROM information_schema.tables
            WHERE table_name = '{tbl}';
        """
        result = self.SQLconnection.execute(sql, return_result=True)

        if result:
            return True
        else:
            return False


    def create_dbtbl(self, tbl):
        """Creates table for insertion! Update this script when you
            want to add more columns to the DB table"""

        sql = f"""
            CREATE TABLE twitterstream.{tbl}(
                twitter_query VARCHAR(100),
            	deploy_timestamp TIMESTAMP,
                id BIGINT PRIMARY KEY,
                created_at TIMESTAMP,
                tweet VARCHAR(250),
                emoji_count INT,
                sentiment FLOAT,
                subjectivity FLOAT
            );
        """
        self.SQLconnection.execute(sql)

    def fetch_times_db(self, tbl):
        """Fetch latest created_at statement"""
        sql = f"""SELECT MAX(id), MAX(created_at) FROM twitterstream.{tbl};"""
        return self.SQLconnection.execute(sql, return_result=True)

    def connect_with_alchemy(self):
        """returns a SQLalchemy engine object"""
        return create_engine(f'mysql+pymysql://{self.SQLconnection.user}:{self.SQLconnection.password}@localhost/{self.SQLconnection.dbname}', pool_recycle=3600)

    def insert_into_dbtbl(self, tbl, rows):
        """Use a SQL alchemy engine to insert a pandas dataframe by the name 'rows'
            into the given database table"""

        SQLEngine = self.connect_with_alchemy()
        engine_connection = SQLEngine.connect()

        #Insert the rows into the database
        rows.to_sql(tbl, con=engine_connection, if_exists='append', index=False)

        engine_connection.close()
