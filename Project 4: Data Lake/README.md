# Project 4: Data Lake

### 1. Summary

---

    A startup called Sparkify has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

    The project objective is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

### 2. How to run

---

- #### You will need to fill the following information (AWS IAM credential), and save it within the `dl.cfg` file in the project root folder.

    <code>
    [AWS]
    AWS_ACCESS_KEY_ID       =
    AWS_SECRET_ACCESS_KEY   =
    
    [IO]
    INPUT   = "s3://udacity-dend"
    OUTPUT  = "s3://udacity-dend/my_results"
    </code>

- #### Run the terminal and run the following script:
    - `$ python etl.py`

### 3. Files in the repository

---

|File Name| Description|
|---------|------------|
|**dl.cfg**|A configuration file that contains key value sets.|
|**etl.py**|Python script that reads data from S3, processes that data using Spark, and writes them back to S3.|
