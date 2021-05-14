import pandas as pd
import pymysql
import cryptography
from keys import db_user, db_password
from getpass import getpass
from mysql.connector import connect, Error
from sqlalchemy import create_engine

class MySQLConnection:
    """
    A class that manages the connection with the given Database.

    ...

    Attributes
    ----------
    host : str
        a string with the role for the user on the DB
    user : str
        the username on the DB (could be root)
    password : str
        a string with the password to connect to the database
    dbname : str
        name of the database we want to connect with

    Methods
    -------
    execute(statement, return_result)
        Executes SQL code on the DB. Returns the result if desired (if=True)

    connect_with_alchemy()
        Creates an alchemy engine class with the given database.

    """

    def __init__(self, dbname='default'):
        self.host="localhost"
        # self.user=input("Enter username: ")
        self.user = db_user
        # self.password=getpass("Enter password: ")
        self.password = db_password
        self.dbname = dbname
        self.port = 3306

    def test_connect(self):
        """Test connection with the DB"""
        try:
            with connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.dbname,
                port = self.port
            ) as connection:
                print(f"achieved to connect with {connection} and database '{self.dbname}''!")
        except Error as e:
                print(e)

    def execute(self, statement, return_result=False):
        """
        Goal: Execute statement on DB

        ----------parameters----------

        statement : str
            The SQL statement to be executed by the cursor

        return_result : boolean
            True if the results need to be returned
        """
        try:
            with connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.dbname,
                port = self.port
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(statement)
                    if return_result:
                        result = cursor.fetchall()
                        return result
        except Error as e:
                print(e)

    def return_cursor(self):
        """return the cursor itself to execute SQL. Not recommended
            since using with statements are best practice with cursors
            and database connections"""

        with connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.dbname,
            port = self.port
        ) as connection:
            return connection.cursor()

    def connect_with_alchemy(self):
        """returns a SQLalchemy engine object"""
        return create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}', pool_recycle=3600)
