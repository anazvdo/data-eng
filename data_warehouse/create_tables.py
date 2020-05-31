import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        Drops tables using statements in sql_queries.py
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        Creates tables using statements in sql_queries.py
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        - Configures dwh.cfg with all important variables
        - Creates Redshift connection
        - Drops all sparkifydb tables
        - Creates all sparkifydb tables
        - Closes Redshift connection 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()