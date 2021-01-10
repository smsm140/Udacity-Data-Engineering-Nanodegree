import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
     This function creates the spark session if it does not exists or gets it and then returns it.
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    This function reads song_data from S3 then processes it forming songs and artists tables
    written into S3 partitioned by some columns if needed in parquet format 
        
    Parameters
    ----------
    spark       : object
                    Spark Session Object
    input_data  : str
                    path of song_data to read
    output_data : str
                    path to the results stored
    '''
    mode='overwrite'
    partitionBy = []
    end = '\n\n'
    
    print_header('Processing Song Data', end=end)
    
    print_header('Reading From S3', char='-')
    
    print(' - Reading song_data files.')
    # filepath to song data file
    songs_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    # read song data file
    songs_df = spark.read.json(songs_data)
    print('   Done.', end=end)

    # create songs view for SQL queries
    songs_df.createOrReplaceTempView("songs_view")
    
    # extract columns to create songs table
    songs_table = spark.sql('''                          
        SELECT DISTINCT
            songs.song_id,
            songs.title, 
            songs.artist_id,
            songs.year,
            songs.duration 
        FROM
            songs_view songs
        WHERE
            songs.song_id IS NOT NULL
    ''')
    print_header('Writing To S3', char='-')
    
    # partition song table by: ['year', 'artist_id']
    partitionBy = ['year', 'artist_id']
    # path for song table output results
    songs_path = os.path.join(output_data,'songs')
    print(' - Writing songs_table in parquet format.')
    # write songs table in parquet files format
    songs_table.write.parquet(songs_path, mode=mode, partitionBy=partitionBy)
    print('   Done.', end=end)
    
    # extract columns to create artists table
    artists_table = spark.sql('''
        SELECT DISTINCT
            artists.artist_id,
            artists.artist_name,
            artists.artist_location,
            artists.artist_latitude,
            artists.artist_longitude
        FROM
            songs_view artists
        WHERE
            artists.artist_id IS NOT NULL
    ''')
    
    print(' - Writing artists_table in parquet format.')
    # path for artists table output results
    artists_path = os.path.join(output_data, 'artists')
    
    # write artists table to parquet files
    artists_table.write.parquet(artists_path, mode=mode)
    print('   Done.', end=end)

def process_log_data(spark, input_data, output_data):
    '''
    This function reads log_data from S3 then processes it forming users, time and songplay tables
    written into S3 partitioned by some columns if needed in parquet format        
    Parameters
    ----------
    spark       : object
                    Spark Session Object
    input_data  : str
                    path of song_data to read
    output_data : str
                    path to the results stored
    '''
    mode='overwrite'
    partitionBy = []
    end = '\n\n'
    
    print_header('Processing Log Data', end=end)
    
    print_header('Reading From S3', char='-')
    
    print(' - Reading log_data files.')
    # filepath to log data file
    log_data_path = os.path.join(input_data, 'log_data/*.json')
    # read log data file
    log_df = spark.read.json(log_data_path)
    print('   Done.',end=end)
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')
    # create timestamp column from original timestamp column
    log_df = log_df.withColumn('log_timestamp', F.to_timestamp(log_df.ts/1000))
    # create datetime column from log_timestamp column
    log_df = log_df.withColumn('log_datetime', F.to_date(log_df.log_timestamp))
    
    # create logs view for SQL queries
    log_df.createOrReplaceTempView("logs_view")
    # extract columns for users table    
    users_table = spark.sql('''
       SELECT DISTINCT
            logs_view.userId AS user_id, 
            logs_view.firstName AS first_name,
            logs_view.lastName AS last_name, 
            logs_view.gender,
            logs_view.level
        FROM
            logs_view
        WHERE
            logs_view.userId IS NOT NULL
    ''')
    
    print_header('Writing To S3', char='-')
    print(' - Writing users_table in parquet format.')
    users_path = os.path.join(output_data, 'users')
    
    # write users table in parquet files format
    users_table.write.parquet(users_path, mode=mode)
    print('   Done.', end=end)
        
    # extract columns to create time table
    time_table = spark.sql('''       
        SELECT 
            logs_view.log_timestamp AS start_time,
            hour(logs_view.log_timestamp) AS hour, 
            dayofmonth(logs_view.log_timestamp) AS day, 
            weekofyear(logs_view.log_timestamp) AS week, 
            month(logs_view.log_timestamp) AS month, 
            year(logs_view.log_timestamp) AS year, 
            dayofweek(logs_view.log_timestamp) AS weekday 
        FROM 
            logs_view
        WHERE
            logs_view.ts IS NOT NULL
    ''')
    
    print(' - Writing time_table in parquet format.')
    # partition time table by: ['year', 'month']
    partitionBy = ['year','month']
    # path for time table output results
    time_path = os.path.join(output_data, 'time')
    # write time table in parquet files format
    time_table.write.parquet(time_path, mode=mode, partitionBy=partitionBy)
    print('   Done.', end=end)
    
    # read in songs data to use for songplays table
    songs_path = os.path.join(output_data, 'songs')
    song_df = spark.read.parquet(songs_path)
    # create songs view for SQL queries
    song_df.createOrReplaceTempView("songs_view")
    
    # read in artists data to use for songplays table
    artists_path = os.path.join(output_data, 'artists')
    artists_df = spark.read.parquet(artists_path)
    # create artists view for SQL queries
    artists_df.createOrReplaceTempView('artists_view')
    
    # extract columns from joined song, artist sand log tables to create songplays table 
    songplays_table = spark.sql('''
        SELECT
            monotonically_increasing_id() AS songplay_id, 
            log.log_timestamp AS start_time, 
            month(log.log_timestamp) AS month, 
            year(log.log_timestamp) AS year, 
            log.userId AS user_id,
            log.level, 
            songs.song_id,
            artists.artist_id, 
            log.sessionId AS session_id,
            log.location, 
            log.userAgent AS user_agent
        FROM 
            logs_view log
            LEFT OUTER JOIN songs_view songs on log.song = songs.title 
            LEFT OUTER JOIN artists_view artists on log.artist = artists.artist_name
    ''')
    
    print(' - Writing songplays_table in parquet format.')
    # partition songplays table by: ['year', 'month']
    partitionBy = ['year','month']
    # path for songplays table output results
    songplays_path = os.path.join(output_data, 'songplays')
    # write time table in parquet files format
    songplays_table.write.parquet(songplays_path, mode=mode, partitionBy=partitionBy)
    print('   Done.', end=end)

def print_header(header, length=0, char='=', start='',end=''):
    '''
    This function prints ``header`` value decorated and formated
    
    Parameters
    ----------
    header  : str
                the string value as header.
    length  : integer
                the length needed for the header to be aligned with.
    char    : str
                a character wanted the line to be printed as, with default value `=`. Ex.: +,-,*,=, ... 
    start   : str
                string value before header
    end     : str
                string value after header
    '''
    header_length = len(header)
    _base_length = 50
    _margins = 2
    # if there is nothing to print then just return
    if header_length < 1:
        return
    
    # if the length is NOT specified or < 0 then assign it to the base length in case:
    # the header length < the base length - margins or to the header length + margins
    if length <= 0:
        length = _base_length if header_length <= (_base_length - _margins) else (header_length + _margins)
    # else the length is specified then it is assigned to header length + margins in case header length >= length
    # or then to length + margins
    else:
        length = (header_length + _margins) if header_length >= length else (length + _margins)
    
    # if char is empty then assign it to '=' else if it is more than a char then get the 1st char
    if len(char) < 1:
        char = '='
    elif len(char) > 1:
        char = char[0]
        
    line_format = '+{:'+char+'<' + str(length) + '}+'
    head_format = '|{:^' + str(length) + '}|'
    line = line_format.format(char)
    head = head_format.format(header)
    print('',end=start)
    print(line)
    print(head)
    print(line)
    print('',end=end)
    
def main():
    '''This is the main function'''
    spark = create_spark_session()
    input_data = config['IO']['INPUT']
    output_data = config['IO']['OUTPUT']
    
    process_song_data(spark, input_data, output_data)   
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
