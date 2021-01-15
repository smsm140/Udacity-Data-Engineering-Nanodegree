# Project 3: Datawarehouse

## 1. Summary

A startup called Sparkify has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The project objective is to build an ETL pipeline that will extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to

## 2. How to run

- You will need to fill the following information, and save it as `dwh.cfg` in the project root folder.

    ```cfg
    [CLUSTER]
    HOST                   =
    DB_NAME                =dwh
    DB_USER                =dwhuser
    DB_PASSWORD            =Passw0rd
    DB_PORT                =5439
    
    [IAM_ROLE]
    ARN                    =
    
    [S3]
    LOG_DATA               ='s3://udacity-dend/log_data'
    LOG_JSONPATH           ='s3://udacity-dend/log_json_path.json'
    SONG_DATA              ='s3://udacity-dend/song_data'
    
    [AWS]
    KEY                    =
    SECRET                 =
    
    [DWH]
    DWH_CLUSTER_TYPE       = multi-node
    DWH_NUM_NODES          = 4
    DWH_NODE_TYPE          = dc2.large
    DWH_IAM_ROLE_NAME      = dwhRole
    DWH_CLUSTER_IDENTIFIER = dwhCluster
    DWH_DB                 = dwh
    DWH_DB_USER            = dwhuser
    DWH_DB_PASSWORD        = Passw0rd
    DWH_PORT               = 5439
    
    [REGIONS]
    REGION                 = 
    ```

- Go through the `STEPS` in `create_custer.ipynb` `1` to `4` to create and lunch the cluster.

- Run the terminal and run these scripts:
  1. `$ python create_tables.py`
  1. `$ python etl.py`

- Go back to `create_custer.ipynb` going through the last step `STEP 5` to clean up created resources ***AFTER YOU'ER DONE FROM EVERYTHING TO PREVENT LOSING MONEY***.

## 3. Database schema design

- Staging Tables
  - staging_events

    ```sql
    CREATE TABLE staging_events(
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
    ```

  - staging_songs

    ```sql
    CREATE TABLE staging_songs(
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
    ```

- Fact Table
  - songplays: records in event data associated with song plays i.e. records with page NextSong.

    ```sql
    CREATE TABLE songplays(
        songplay_id         INTEGER         IDENTITY(0,1)   PRIMARY KEY,
        start_time          TIMESTAMP,
        user_id             INTEGER,
        level               VARCHAR,
        song_id             VARCHAR,
        artist_id           VARCHAR,
        session_id          INTEGER,
        location            VARCHAR,
        user_agent          VARCHAR
    )
    ```

- Dimension Tables
  - users: users in the app.

    ```sql
    CREATE TABLE users(
        user_id             INTEGER PRIMARY KEY,
        first_name          VARCHAR,
        last_name           VARCHAR,
        gender              VARCHAR,
        level               VARCHAR
    )
    ```

  - songs: songs in music database.

    ```sql
    CREATE TABLE songs(
        song_id             VARCHAR PRIMARY KEY,
        title               VARCHAR,
        artist_id           VARCHAR,
        year                INTEGER,
        duration            FLOAT
    )
    ```

  - artists: artists in music database.

    ```sql
    CREATE TABLE artists(
        artist_id           VARCHAR  PRIMARY KEY,
        name                VARCHAR,
        location            VARCHAR,
        latitude            FLOAT,
        longitude           FLOAT
    )
    ```

  - time: timestamps of records in songplays broken down into specific units.

    ```sql
    CREATE TABLE time(
        start_time          TIMESTAMP       NOT NULL PRIMARY KEY,
        hour                INTEGER         NOT NULL,
        day                 INTEGER         NOT NULL,
        week                INTEGER         NOT NULL,
        month               INTEGER         NOT NULL,
        year                INTEGER         NOT NULL,
        weekday             VARCHAR(20)     NOT NULL
    )
    ```

## 4. Files in the repository

|File Name| Description|
|---------|------------|
|**create_cluster.ipynb**|A Jupyter notebook steps to create and launch the redshift cluster.|
|**create_tables.py**|Python script to drop and create tables by running drop and create SQL statements.|
|**dwh.cfg**|A configuration file that contains key value sets.|
|**etl.py**|Python script to load data to staging tables from s3 buckets then perform ETL pipline for songplays, users, songs, artists and time table.|
|**sql_queries.py**|Python file containing SQL queries in variables to be imported for python scripts and run to drop and create database and tables.|