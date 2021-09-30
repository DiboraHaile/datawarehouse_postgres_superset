import pymysql
import os
import pandas as pd

## What we are trying to do is read mysql db, fetch data maybe return that
## with dataframe then feed that to postgres db
class fetch_from_mysql:
    """ Fetch all the data from mysql and return it in dataframe"""
    def __init__(self,db):
        self.db = db
        
    def create_connection(self):
        connection = pymysql.connect(host='localhost',
                                                user='root',
                                                database = self.db,
                                                password='adminadmin')

        cursor = connection.cursor()
        return connection, cursor

    def return_tables(self):
        conn, cur = self.create_connection()
        cur.execute("show tables")
        tables = cur.fetchall()
        values = []
        for table_name in tables:
            values.append(list(table_name))
        order = [5,6,0,1,2,3,4]
        values = [values[o] for o in order]
        cur.close()
        return values

    def get_schema(self):
        _,cur = self.create_connection()
        tables = self.return_tables()
        metadata = []
        for table in tables:
            cur.execute(f"SHOW CREATE TABLE {table[0]}")
            metadata.append(cur.fetchall())
        cur.close()
        return metadata


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
#     f = fetch_from_mysql("Sensor_raw_db")
#     print(f.return_tables())