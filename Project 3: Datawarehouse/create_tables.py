import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    
    Parameters
    ----------
    cur         : Cursor object
                    Cursor connected to a database session
    conn        : Connection object
                    Session connection to a database
    """
    print('***************************** Dropping Tables *****************************')
    for table in drop_table_queries:
        print('- Dropping {} Table'.format(table['name']))
        cur.execute(table['query'])
        conn.commit()
        print('  Done.')
    print('***************************************************************************', end='\n\n')

def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    
    Parameters
    ----------
    cur         : Cursor object
                    Cursor connected to a database session
    conn        : Connection object
                    Session connection to a database
    """
    print('***************************** Creating Tables *****************************')
    for table in create_table_queries:
        print('- Creating {} Table'.format(table['name']))
        cur.execute(table['query'])
        conn.commit()
        print('  Done.')
    print('***************************************************************************', end='\n\n')

def main():
    """
    - Establishs database connection and gets cursor to it.
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
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