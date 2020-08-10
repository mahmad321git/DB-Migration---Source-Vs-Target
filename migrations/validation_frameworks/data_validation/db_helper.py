"""
----------------------------------------------------------------------------------------------------------
Description:

usage: DB Helper Methods

Author  : Fahad Anayat Khan/Ghufran Ahmad

Modification Log:

How to execute:
-----------------------------------------------------------------------------------------------------------
Date                Author              Story               Description
-----------------------------------------------------------------------------------------------------------
05/8/2019        Fahad Anayat Khan                          Initial draft.
05/8/2019        Ghufran Ahmad                              Initial draft.
03/13/2020       Salman Haroon                              Logging Implementation
-----------------------------------------------------------------------------------------------------------
"""
import sqlalchemy as db
import pandas as pd
import json
import logging
#import cx_Oracle



class DbHelper:
    def __init__(self):
        pass

    def create_conn(self, conn_string):
        """
               Create connection with database.
               :param conn_string: Connection String required to build connection.
        """
        try:
            self.database_engine = db.create_engine(conn_string)
            self.db_connection = self.database_engine.connect()
            self.db_metadata = db.MetaData()
        except ConnectionError as e:
            logging.error(e)

    def query_execution(self, query):
        """
            Execute query on database.
            :param query: Required Query.
        """
        try:
            df = pd.read_sql_query(query, self.db_connection)
            return df
        except ConnectionError as e:
            logging.error(e)

    def generate_connection_string(self, database, username, passw, host, port, dbname, sqlite_db_path):
        """
            Generate a connection string to connect with database.
            :param database: Database (mysql, postgresql, oracle).
            :param username: Db username
            :param  passw: Db Password
            :param  host: Db host
            :param  port: Port # required
            :param  dbname: Database Name/ Service Name (ORACLE)
            :param sqlite_db_path: Path for sqlite db file.
        """
        conn_string = ""
        if database == "mysql":
            conn_string = database+"+pymysql://"+username+":"+passw+"@"+host+":"+port+"/"+dbname
        elif database == "postgresql":
            conn_string = database + "+psycopg2://" + username + ":" + passw + "@" + host + ":" + port + "/" + dbname
        elif database == "oracle":
            #conn_string = database + "+cx_oracle://" + username + ":" + passw + "@" + host + ":" + port + "/" + dbname
            conn_string = database + "+cx_oracle://" + username + ":" + passw + "@" + host + ":" + port + "/" + dbname
            # dsn = '%s:%s/%s' % (host, port, dbname)
            # conn_string = cx_Oracle.connect(username,passw,dsn)
        elif database == "mariadb":
            conn_string = database + "+mariadb://" + username + ":" + passw + "@" + host + ":" + port + "/" + dbname
        elif database == "mssql":
            conn_string = database + "+pymssql://" + username + ":" + passw + "@" + host + ":" + port + "/" + dbname
        elif database == "sqlite":
            conn_string = "'"+database+"'///"+sqlite_db_path
        return conn_string
