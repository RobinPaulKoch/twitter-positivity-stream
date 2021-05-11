import pandas as pd
import pymysql
import cryptography
from config import db_user, db_password
from getpass import getpass
from mysql.connector import connect, Error
from sqlalchemy import create_engine

class dbtraffic:
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
    execute(statement, return_result)
        Executes SQL code on the DB. Returns the result if desired (if=True)

    connect_with_alchemy()
        Creates an alchemy engine class with the given database.

    """

    def __init__(self, SQLconnection):

        self.SQLconnection=SQLconnection


    # Functions to facilitate data exchange with the database
    def create_output_table(self, rows):
        sql = f"""
            SELECT * FROM information_schema.tables
            WHERE table_name = {tbl};
        """
        self.SQLconnection.execute(sql)


    def check_tbl_exists(self, tbl):

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
        self.SQLconnection.execute(sql)

    def fetch_times_db(self, tbl):
        sql = f"""SELECT MAX(id), MAX(created_at) FROM twitterstream.{tbl};"""
        return self.SQLconnection.execute(sql, return_result=True)

    def connect_with_alchemy(self):
        return create_engine(f'mysql+pymysql://{self.SQLconnection.user}:{self.SQLconnection.password}@localhost/{self.SQLconnection.dbname}', pool_recycle=3600)

    def insert_into_dbtbl(self, tbl, rows):

        SQLEngine = self.connect_with_alchemy()
        engine_connection = SQLEngine.connect()

        #Insert the rows into the database
        rows.to_sql(tbl, con=engine_connection, if_exists='append', index=False)

        engine_connection.close()
