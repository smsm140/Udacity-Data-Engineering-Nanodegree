# Project 1: Data Modeling with Postgres

### 1. Summary

---
    A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

- #### Database Schema Design
    A **star-schema** is used here to model data whitch results in fact and dimension tables.
    - Fact Table:
        - songplays
    - Dimension Tables:
        - users
        - songs
        - artists
        - time
- #### ETL Pipeline
    An ETL pipeline is the process of extracting the needed data, transforming them as needed and loading them to the appropriate table which is implemented in `etl.py` for dimension and fact tables above.

### 2. How to run the python scripts

---
Run the following python files on the terminal **respectively**.
1. Run `python create_tables.py` to create database and tables

2. Run `python etl.py` to perform an ETL pipeline

### 3. Files in the repository

---

|File Name| Description|
|---------|------------|
|**data**|Data directory that contains song and log data files.|
|**create_tables.py**|Python script to drop and create database and tables by running drop and create SQL statements.|
|**etl.ipynb**|A jupyter notbook performing ETL pipeline as a blueprint for the etl.py.|
|**etl.py**|Python script to perform ETL pipeline **extract** the needed data from song and log data files within song_data and log_data directories, **transform** them if needed and then **load** data to its appropriate table within the created database schema.|
|**sql_queries.py**|Python file containing SQL queries in variables to be imported for python scripts and run to drop and create database and tables.|
|**test.ipynb**|A jupyter notebook to verify the creation of database & tables and insertion of data correctly.|