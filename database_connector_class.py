import pandas as pd
from getpass import getpass
from mysql.connector import connect, Error
from sqlalchemy import create_engine
import pymysql
import cryptography

class MySQLConnection:
    def __init__(self, dbname='default'):
        self.host="localhost"
        self.user=input("Enter username: ")
        self.password=getpass("Enter password: ")
        self.dbname = dbname

    def test_connect(self):
        try:
            with connect(
                host="localhost",
                user=self.user,
                password=self.password,
                database=self.dbname
            ) as connection:
                print(f"achieved to connect with {connection} and database '{self.dbname}''!")
        except Error as e:
                print(e)

    def execute(self, statement, return_result=False):
        try:
            with connect(
                host="localhost",
                user=self.user,
                password=self.password,
                database=self.dbname
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(statement)
                    if return_result:
                        result = cursor.fetchall()
                        return result
        except Error as e:
                print(e)

    def return_cursor(self):
        with connect(
            host="localhost",
            user=self.user,
            password=self.password,
            database=self.dbname
        ) as connection:
            return connection.cursor()

    def connect_with_alchemy(self):
        return create_engine(f'mysql+pymysql://{self.user}:{self.password}@localhost/{self.dbname}', pool_recycle=3600)
