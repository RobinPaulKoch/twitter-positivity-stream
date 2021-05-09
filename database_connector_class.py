import pandas as pd
from getpass import getpass
from mysql.connector import connect, Error

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
