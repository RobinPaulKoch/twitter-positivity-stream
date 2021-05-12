from configurations import config
from library.database_connector_class import MySQLConnection

DBNAME = config.DBNAME
TBLNAME = config.TBLNAME
SEARCHQ = config.SEARCHQ
UTCTIME_DIFF = config.UTCTIME_DIFF #If you want to transfer the retrieved dates to "localtime"

if __name__ == "__main__":
    #Set up the MySQL connection:
    SQLconnection = MySQLConnection(DBNAME)
    SQLconnection.test_connect()
