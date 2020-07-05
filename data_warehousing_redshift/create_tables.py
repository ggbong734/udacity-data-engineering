import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Drop existing tables from Redshift database.
    
    Keyword arguments:
    cur: cursor to the database. Allows execution of SQL commands.
    conn: connection to the Redshift database using psycopg2.
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Create new tables including staging tables and the fact and dimension tables.
    
    Keyword arguments:
    cur: cursor to the database. Allows execution of SQL commands.
    conn: connection to the Redshift database using psycopg2.
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Connect to Redshift database, drop existing tables, create new tables.
    
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()