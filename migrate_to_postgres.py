import sqlalchemy
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
from mysql_query import fetch_from_mysql
# import streamlit as st

class insert_postgres:
    def __init__(self,db,table,fetched_df):
        self.db = db
        self.table = table
        self.fetched_df = fetched_df
    
    def create_connection(self):
        p_engine = create_engine(f"postgresql://dibora:adminadmin@localhost:5432/{self.db}")
        # p_engine.execute("CREATE TABLE IF NOT EXISTS records (name text PRIMARY KEY, details text[])")
        return p_engine

    def write_record(self):
        p_engine = self.create_connection()
        columns_list = self.fetched_df.columns.tolist()
        colmns_name = ",".join(columns_list)
        no_values = ",".join(["%s" for i in range(len(columns_list))])

        query = f"INSERT INTO {self.table} ({colmns_name}) VALUES ({no_values})"
        list_rows = []
        for _,row in self.fetched_df.iterrows():
            list_rows.append(tuple([row[i] for i in range(len(columns_list))]))
        p_engine.execute(query, list_rows)
        






# if __name__ == "__main__":
#     df = fetch_from_mysql('example').fetch_data('records_new')
#     print(df.head())
#     ip = insert_postgres("example","data_new",df)
#     ip.write_record()
