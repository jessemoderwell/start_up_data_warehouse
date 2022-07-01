import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    '''Drops all existing tables in redshift, if they exist
    
    Keyword arguments:
    cur -- the cursor for our database connection
    conn -- variable representing our current connection
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    '''Creates staging and star schema tables in redshift 
    
    This creates these tables only if they don't exist yet
    cur -- the cursor for our database connection
    conn -- variable representing our current connection
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Connected to database")
    
    drop_tables(cur, conn)
    print("Any existing tables have been dropped")
    create_tables(cur, conn)
    print("Tables have been created")

    conn.close()

if __name__ == "__main__":
    main()