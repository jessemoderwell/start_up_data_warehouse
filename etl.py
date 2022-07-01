import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    '''Uses copy commamd to copy CSVs in s3 to staging tables
    
    Keyword arguments:
    cur -- the cursor for our database connection
    conn -- variable representing our current connection
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    '''Inserts data from staging tables to star schema tables
    
    Keyword arguments:
    cur -- the cursor for our database connection
    conn -- variable representing our current connection
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    print("Staging tables have been loaded")
    insert_tables(cur, conn)
    print("Inserts complete")
    conn.close()


if __name__ == "__main__":
    main()