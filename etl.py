import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# ---------------------------------------------------------
# This script performs the ETL pipeline:
# 1. Load data from S3 to staging tables (COPY)
# 2. Transform and insert into star schema tables (INSERT)
# ---------------------------------------------------------

def load_staging_tables(cur, conn):
    """
    Loads data from S3 into staging tables in Redshift
    using the COPY statements defined in sql_queries.py
    """
    for query in copy_table_queries:
        print(f"Running COPY query:\n{query}\n")
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    Inserts data from staging tables into the analytics tables
    (fact and dimension tables) using INSERT queries.
    """
    for query in insert_table_queries:
        print(f"Running INSERT query:\n{query}\n")
        cur.execute(query)
        conn.commit()

def main():
    """
    - Reads connection config from dwh.cfg
    - Connects to Redshift using psycopg2
    - Executes data load from S3 to staging tables
    - Executes inserts from staging to star schema tables
    - Closes the connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Establish connection using credentials from the [CLUSTER] section
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Perform data load and transformation
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # Close Redshift connection
    conn.close()

if __name__ == "__main__":
    main()
