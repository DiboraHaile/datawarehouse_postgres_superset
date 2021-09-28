import mysql.connector
from mysql.connector import Error
import os
import pandas as pd

## What we are trying to do is read mysql db, fetch data maybe return that
## with dataframe then feed that to postgres db
class fetch_from_mysql:
    """ Fetch all the data from mysql and return it in dataframe"""
    def __init__(self,db):
        self.db = db
        
    def create_connection(self):
        connection = mysql.connector.connect(host='localhost',
                                                database=self.db,
                                                user='root',
                                                password='adminadmin')

        cursor = connection.cursor()
        return connection, cursor

    def fetch_data(self,table_name):
        conn, cur = self.create_connection()
        colmn_names = []
        values = []
        select_query = "select * FROM "+table_name

        cur.execute(select_query)
        for row,desc in zip(cur,cur.description):
            values.append(row)
            colmn_names.append(desc[0])
        
        
        df = pd.DataFrame(values, columns=colmn_names)
        conn.commit()
        cur.close()
        return df

# if __name__ == "__main__":
#     fmsyql = fetch_from_mysql("example")
#     print(fmsyql.fetch_data('records').head())
