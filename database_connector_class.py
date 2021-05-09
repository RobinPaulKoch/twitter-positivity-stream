import pandas as pd
from getpass import getpass
from mysql.connector import connect, Error
from sqlalchemy import create_engine
import pymysql
import cryptography

class MySQLConnection:
    def __init__(self):
        self.host="localhost"
        self.user=input("Enter username: ")
        self.password=getpass("Enter password: ")

    def test_connect(self):
        try:
            with connect(
                host="localhost",
                user=self.user,
                password=self.password,
            ) as connection:
                print(f"achieved to connect with {connection}!")
        except Error as e:
                print(e)

    def execute(self, statement):
        try:
            with connect(
                host="localhost",
                user=self.user,

                password=self.password,
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(statement)
        except Error as e:
                print(e)

    def return_cursor(self):
        with connect(
            host="localhost",
            user=self.user,
            password=self.password,
        ) as connection:
            return connection.cursor()

    def connect_with_alchemy(self, dbname):
        return create_engine(f'mysql+pymysql://{self.user}:{self.password}@localhost/{dbname}', pool_recycle=3600)
