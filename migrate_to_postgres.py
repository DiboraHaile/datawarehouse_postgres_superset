from sqlalchemy import create_engine
import psycopg2
import pandas as pd
from fetch_from_mysql import fetch_from_mysql


class insert_postgres:
    """ Accept the fetched data and insert to postgres"""
    def __init__(self,db):
        self.db = db
        
    
    def create_connection(self):
        p_engine = create_engine(f"postgresql://dibora:adminadmin@localhost:5432/{self.db}")
        return p_engine


    def create_tables(self):
        p_engine = self.create_connection()
        f= fetch_from_mysql(self.db)
        tables = f.return_tables()
        metadata = f.get_schema()
        for i in range(len(tables)):
            meta = metadata[i][0][1]
            meta = meta.replace("`","").split("ENGINE")
            query_words = meta[0].split(",")

            for col in query_words:
                if col.split()[0] == "KEY":
                    query_words.remove(col)


            create_query = ",".join(query_words)
            print(create_query)
            p_engine.execute(create_query)


    def write_record(self):
        p_engine = self.create_connection()
        f = fetch_from_mysql(self.db)
        for table in f.return_tables():
            fetched_df = f.fetch_data(table[0])
            columns_list = fetched_df.columns.tolist()
            colmns_name = ",".join(columns_list)
            no_values = ",".join(["%s" for i in range(len(columns_list))])

            query = f"INSERT INTO {self.db}.{self.table} ({colmns_name}) VALUES ({no_values})"
            list_rows = []
            for _,row in fetched_df.iterrows():
                list_rows.append(tuple([row[i] for i in range(len(columns_list))]))
            p_engine.execute(query, list_rows)
        






# if __name__ == "__main__":
#     ip = insert_postgres("Sensor_raw_db")
#     ip.create_tables()

