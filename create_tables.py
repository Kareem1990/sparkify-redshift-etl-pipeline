# pip install psycopg2-binary

import configparser  # Used to read configuration from the dwh.cfg file
import psycopg2      # PostgreSQL adapter to connect to Redshift
from sql_queries import create_table_queries, drop_table_queries
# These two lists are imported from sql_queries.py and contain all DROP/CREATE SQL statements

# ---------------------------------------------------------
# This script connects to the Redshift cluster and executes
# DROP and CREATE statements to manage all necessary tables.
# ---------------------------------------------------------

def drop_tables(cur, conn):
    """
    Drops all tables listed in the drop_table_queries list.
    These queries are defined in sql_queries.py and include:
    - staging_events
    - staging_songs
    - songplays
    - users
    - songs
    - artists
    - time
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Creates all tables listed in the create_table_queries list.
    These are also defined in sql_queries.py and include:
    - staging tables (raw S3 data)
    - analytics tables (fact & dimension tables in the star schema)
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    - Reads Redshift credentials from dwh.cfg under the [CLUSTER] section
    - Connects to Redshift using psycopg2
    - Drops all existing tables
    - Creates all required tables
    - Closes the Redshift database connection
    """

    # Load the dwh.cfg file (make sure it's in the same folder)
    config = configparser.ConfigParser()
    config.read('dwh.cfg', encoding='utf-8')

    # The [CLUSTER] section in dwh.cfg should contain:
    # HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
    # Example:
    # [CLUSTER]
    # HOST=your-cluster.redshift.amazonaws.com
    # DB_NAME=dwh
    # DB_USER=dwhuser
    # DB_PASSWORD=yourpassword
    # DB_PORT=5439

    # Connect to the Redshift cluster using the values from the config
    conn = psycopg2.connect(
    host=config.get("CLUSTER", "HOST"),
    dbname=config.get("CLUSTER", "DB_NAME"),
    user=config.get("CLUSTER", "DB_USER"),
    password=config.get("CLUSTER", "DB_PASSWORD"),
    port=config.get("CLUSTER", "DB_PORT"))

    cur = conn.cursor()

    # Drop and recreate tables
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # Always close your connection when done
    conn.close()

# Entry point when the script is run directly
if __name__ == "__main__":
    main()
