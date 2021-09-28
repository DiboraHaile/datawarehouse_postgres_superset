import sqlalchemy
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
from fetch_from_mysql import fetch_from_mysql


class insert_postgres:
    """ Accept the fetched data and insert to postgres"""
    def __init__(self,db,table):
        self.db = db
        self.table = table

        
    
    def create_connection(self):
        p_engine = create_engine(f"postgresql://dibora:adminadmin@localhost:5432/{self.db}")
        return p_engine


    def create_tables(self):
        p_engine = self.create_connection()
        f= fetch_from_mysql('example')
        tables = f.return_tables()
        metadata = f.get_schema()
        for i in range(len(tables)):
            meta = " ".join(list(metadata[0][i].values())[:2])
            if meta[3] == "PRI":
                meta += " PRIMARY KEY"
            create_query = f"CREATE TABLE IF NOT EXISTS {tables[i][0]} ({meta})"
            p_engine.execute(create_query)


    def write_record(self,fetched_df):
        p_engine = self.create_connection()
        columns_list = self.fetched_df.columns.tolist()
        colmns_name = ",".join(columns_list)
        no_values = ",".join(["%s" for i in range(len(columns_list))])

        query = f"INSERT INTO {self.db}.{self.table} ({colmns_name}) VALUES ({no_values})"
        list_rows = []
        for _,row in self.fetched_df.iterrows():
            list_rows.append(tuple([row[i] for i in range(len(columns_list))]))
        p_engine.execute(query, list_rows)
        






if __name__ == "__main__":
    f= fetch_from_mysql('example')
    ip = insert_postgres("example","data_new")
    ip.create_tables()
    print(ip.write_record)
    # ip.write_record()
