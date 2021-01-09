import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA_BUCKET=config['S3']['LOG_DATA']
SONG_DATA_BUCKET=config['S3']['SONG_DATA']
ROLE_ARN=config['IAM_ROLE']['ARN']
LOG_JSON_PATH=config['S3']['LOG_JSONPATH']
REGION=config['REGIONS']['REGION']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INTEGER,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INTEGER,
        song                VARCHAR,
        status              INTEGER,
        ts                  TIMESTAMP,
        userAgent           VARCHAR,
        userId              INTEGER 
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id         INTEGER         IDENTITY(0,1)   PRIMARY KEY,
        start_time          TIMESTAMP,
        user_id             INTEGER ,
        level               VARCHAR,
        song_id             VARCHAR,
        artist_id           VARCHAR ,
        session_id          INTEGER,
        location            VARCHAR,
        user_agent          VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id             INTEGER PRIMARY KEY,
        first_name          VARCHAR,
        last_name           VARCHAR,
        gender              VARCHAR,
        level               VARCHAR
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id             VARCHAR PRIMARY KEY,
        title               VARCHAR ,
        artist_id           VARCHAR ,
        year                INTEGER ,
        duration            FLOAT
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id           VARCHAR  PRIMARY KEY,
        name                VARCHAR ,
        location            VARCHAR,
        latitude            FLOAT,
        longitude           FLOAT
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time          TIMESTAMP       NOT NULL PRIMARY KEY,
        hour                INTEGER         NOT NULL,
        day                 INTEGER         NOT NULL,
        week                INTEGER         NOT NULL,
        month               INTEGER         NOT NULL,
        year                INTEGER         NOT NULL,
        weekday             VARCHAR(20)     NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {} credentials 'aws_iam_role={}' region '{}' format as JSON {} timeformat as 'epochmillisecs';
""").format(LOG_DATA_BUCKET, ROLE_ARN, REGION, LOG_JSON_PATH)

staging_songs_copy = ("""
    copy staging_songs from {} credentials 'aws_iam_role={}' region '{}' format as JSON 'auto';
""").format(SONG_DATA_BUCKET, ROLE_ARN, REGION)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT DISTINCT
        se.ts AS start_time,
        se.userId AS user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId AS session_id,
        se.location,
        se.userAgent AS user_agent
    FROM
        staging_events se 
        INNER JOIN staging_songs ss ON se.song = ss.title
    WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        se.userId AS user_id,
        se.firstName AS first_name,
        se.lastName AS last_name,
        se.gender,
        se.level
    FROM
        staging_events se
    WHERE
        se.userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration) 
    SELECT DISTINCT
        ss.song_id,
        ss.title,
        ss.artist_id,
        ss.year,
        ss.duration
    FROM
        staging_songs ss
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        ss.artist_id, 
        ss.artist_name AS name, 
        CASE WHEN ss.artist_location IS NULL THEN 'N/A' ELSE ss.artist_location END AS location, 
        CASE WHEN ss.artist_latitude IS NULL THEN 0.0 ELSE ss.artist_latitude END AS latitude, 
        CASE WHEN ss.artist_longitude IS NULL THEN 0.0 ELSE ss.artist_longitude END AS longitude
    FROM
        staging_songs ss
    WHERE
        ss.artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        se.ts AS start_time,
        extract(hour from se.ts) AS hour ,
        extract(day from se.ts) AS day ,
        extract(week from se.ts) AS week ,
        extract(month from se.ts) AS month ,
        extract(year from se.ts) AS year ,
        extract(dayofweek from se.ts) AS weekday 
    FROM
        staging_events se
    WHERE
        se.page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [
    {'name': 'staging_events', 'query': staging_events_table_create},
    {'name': 'staging_songs', 'query': staging_songs_table_create},
    {'name': 'songplay', 'query': songplay_table_create},
    {'name': 'user', 'query': user_table_create},
    {'name': 'song', 'query': song_table_create},
    {'name': 'artist', 'query': artist_table_create},
    {'name': 'time', 'query': time_table_create}
]
drop_table_queries = [
    {'name':'staging_events','query':staging_events_table_drop},
    {'name':'staging_songs','query':staging_songs_table_drop},
    {'name':'songplay','query':songplay_table_drop},
    {'name':'user','query':user_table_drop},
    {'name':'song','query':song_table_drop},
    {'name':'artist','query':artist_table_drop},
    {'name':'time','query':time_table_drop}
]
copy_table_queries = [
    {'name':'staging_events','query':staging_events_copy},
    {'name':'staging_songs','query':staging_songs_copy}
]
insert_table_queries = [
    {'name':'songplay','query':songplay_table_insert},
    {'name':'user','query':user_table_insert},
    {'name':'song','query':song_table_insert},
    {'name':'artist','query':artist_table_insert},
    {'name':'time','query':time_table_insert}
]
