import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This function processes a json song file by reading the file data, select song & atrist data
    then load these data into the appropriate tables.
    
    Parameters
    ----------
    cur         : Cursor object
                    Cursor connected to postgreSQL session
    filepath    : string
                    Path to song file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # columns needed for song table
    song_columns = ["song_id", "title", "artist_id", "year", "duration"]
    
    # insert song record
    song_data = df[song_columns].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # columns needed for artists table
    artist_columns = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
        
    # insert artist record
    artist_data = df[artist_columns].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This function processes a json log file by reading the file data, select time, user and songplay data
    then load these data into the appropriate tables.
    
    Parameters
    ----------
    cur         : Cursor object
                    Cursor connected to postgreSQL session
    filepath    : string
                    Path to log file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'],unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(dict(zip(column_labels,time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # columns needed for user table
    user_columns = ["userId", "firstName", "lastName", "gender", "level"]
    
    # load user table
    user_df = df[user_columns]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            pd.to_datetime(row.ts,unit='ms'),
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This function all json files on a directory and its sub-directories one by one by calling back the passed function
    
    Parameters
    ----------
    cur         : Cursor object
                    Cursor connected to postgreSQL session
    conn        : Connection object
                    Session connection to postgreSQL database
    filepath    : string
                    Path to data driectory that contains data files
    func        : function
                    A callback function to process a specific data file to be extracted, transformed and loaded (ETL)
                    to the appropriate tables
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    This is the main function that processes song and log data
    by performing ETL pipeline
    """
    
    # connect to the database and get the connection and cursor objects
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    # process data files
    print('Processing song data')
    print('====================')
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    print('\nProcessing log data')
    print('===================')
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    # close connection
    conn.close()


if __name__ == "__main__":
    main()