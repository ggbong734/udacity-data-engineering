import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Copy data from S3 bucket and place them onto the staging tables in Redshift database.
    
    Keyword arguments:
    cur: cursor to the database. Allows execution of SQL commands.
    conn: connection to the Redshift database using psycopg2.
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Transform data in the staging tables and move them to the fact and dimension tables in Redshift.
    
    Keyword arguments:
    cur: cursor to the database. Allows execution of SQL commands.
    conn: connection to the Redshift database using psycopg2.
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Connect to Redshift database, load data from S3 buckets to staging tables, and transform the data into fact and dimension tables.
    
    The following are extracted from dwh.cfg file
    host: Redshift cluster endpoint addresss
    dbname: Redshift database name
    user: database username
    password: database password
    port: port to connect to database
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()