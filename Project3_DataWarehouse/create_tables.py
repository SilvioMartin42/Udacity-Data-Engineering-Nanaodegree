import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop tables from drop_table_queries list in sql_queries.py.
    
    Keyword Arguments:
    cur -- cursor to execute query in database
    conn -- psycopg2 connection to database
    
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create tables from create_table_queries list in sql_queries.py.
    
    Keyword Arguments:
    cur -- cursor to execute query in database
    conn -- psycopg2 connection to database
    
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Main function
    
    * parses .cfg file for database credentials
    * creates connection to database
    * drops and creates tables from lists in sql_queries.py
    * closes connection to database
    
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