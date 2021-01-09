import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function loads staging_songs, staging_events tables by copping data from s3 bucket file data.
    
    Parameters
    ----------
    cur         : Cursor object
                    Cursor connected to a database session
    conn        : Connection object
                    Session connection to a database
    """
    
    print('************************* Loading Staging Tables **************************')
    for table in copy_table_queries:
        print('- Loading data into {} Table'.format(table['name']))
        cur.execute(table['query'])
        conn.commit()
        print('  Done.')
    print('***************************************************************************', end='\n\n')

def insert_tables(cur, conn):
    """
    This function insertes data form staging tables into fact and dimension tables
    
    Parameters
    ----------
    cur         : Cursor object
                    Cursor connected to a database session
    conn        : Connection object
                    Session connection to a database
    """
    
    print('************** Inserting Data Into Fact and Dimension Tables **************')
    for table in insert_table_queries:
        print('- Inserting data into {} Table'.format(table['name']))
        cur.execute(table['query'])
        conn.commit()
        print('  Done.')
    print('***************************************************************************', end='\n\n')

def main():
    """
    - Establishs database connection and gets cursor to it.
    
    - Load staging tables.  
    
    - Insert data into tables. 
    
    - Finally, closes the connection. 
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