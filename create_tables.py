import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Input parameters: Takes the cursor object and connection object and executes drop statements from the drop_table_queries list
    return : Not Applicable
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Input parameters: Takes the cursor object and connection object and executes create statements from the create_table_queries list
    return : Not Applicable
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    
    '''
    Getting all the parameter values from the config file and passing to create connection and cursor objects. 
    ''' 
    host = config.get("CLUSTER","HOST")
    dbname = config.get("CLUSTER","DB_NAME")
    user = config.get("CLUSTER", "DB_USER")
    password = config.get("CLUSTER", "DB_PASSWORD")
    port = config.get("CLUSTER", "DB_PORT")
    
    conn = psycopg2.connect(f"host={host} dbname={dbname} user={user} password={password} port={port}")
    cur = conn.cursor()
    
    '''
    Invoking drop_tables function with the above create cursor object and connection object and 
    invoking create_tables functions which execute create queries from the list to create required tables. 
    '''
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()