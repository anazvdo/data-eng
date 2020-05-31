import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
      Loads all files inside S3 on Redshift with COPY FROM

      'cur': psycopg2 cursor from sparkify_dw
      'filepath': string with filepath of song file      
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
      Loads all files inside staging tables to star schema on Redshift with INSERT INTO

      'cur': psycopg2 cursor from sparkify_dw
      'filepath': string with filepath of song file      
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        Using sparkify_dw connection, the ETL process is done here:
        - Process data from song files to be staged on Redshift
        - Process data from log files to be staged on Redshift
        - Loads all staged data to tables in a Star Schema
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()