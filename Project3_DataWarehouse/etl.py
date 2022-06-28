import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load data from external storage (here S3) into staging tables in database.
        
    Keyword Arguments:
    cur -- cursor to execute query in database
    conn -- psycopg2 connection to database
    
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from staging tables into final tables.
        
    Keyword Arguments:
    cur -- cursor to execute query in database
    conn -- psycopg2 connection to database
    
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Main function
    
    * parses .cfg file for database credentials
    * creates connection to database
    * executes load_staging_tables function
    * executes insert_tables function
    * closes connection to database
    
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