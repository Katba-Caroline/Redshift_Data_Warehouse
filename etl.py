import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 files into the staging tables using the queries from sql_queries.py
    """
    print('Loading Data from Raw zone to Staging zone')
    for query in copy_table_queries:
        print('Currently Running ' + query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Move data from staging "zone" into the analytical "zone" 
    """ 
    print('Loading Data from Staging zone to Analytical zone')
    for query in insert_table_queries:
        print('Currently Running ' + query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()