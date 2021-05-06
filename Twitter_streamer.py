import pandas as pd
from getpass import getpass
from mysql.connector import connect, Error

class mysql_connection:
    def __init__(self):
        self.host="localhost",
        self.user=input("Enter username: "),
        self.password=getpass("Enter password: ")

    def test_connect(self):
        try:
            with connect(
                host="localhost",
                user=self.user[0],
                password=self.password,
            ) as connection:
                print(f"achieved to connect with {connection}!")
        except Error as e:
                print(e)


    def execute(self, statement):
        try:
            with connect(
                host="localhost",
                user=self.user[0],
                password=self.password,
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(statement)
        except Error as e:
                print(e)


sql_connector = mysql_connection()
sql_connector.test_connect()

sql_connector.execute("DROP DATABASE online_movie_rating")


del sql_connector

test = input("Enter username: ")
test

try:
    with connect(
        host="localhost",
        user=input("Enter username: "),
        password=getpass("Enter password: "),
    ) as connection:
        print("jeej")
        # create_db_query = "DROP DATABASE online_movie_rating"
        # with connection.cursor() as cursor:
        #     cursor.execute(create_db_query)
except Error as e:
    print(e)

try:
    with connect(
        host="localhost",
        user=sql_connector.user[0],
        password=sql_connector.password,
    ) as connection:
        print("jeej")
        # create_db_query = "DROP DATABASE online_movie_rating"
        # with connection.cursor() as cursor:
        #     cursor.execute(create_db_query)
except Error as e:
    print(e)
