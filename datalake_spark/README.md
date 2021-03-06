## Data Lake with Spark - Sparkify 

### Introduction 

<i>Sparkify</i> is a startup that owns a new music streaming app.
They've collected some data about songs and user activity on this app. All their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

### Objectives

<i>Sparkify</i> needs to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. 

### Datasets

- **Songs**: is a subset of real data from [Million Song Dataset](http://millionsongdataset.com/). The files reside in s3://udacity-dend/song_data and looks like this:

```
 {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0} </code>
 ```

- **Logs**: generated by [event simulator](https://github.com/Interana/eventsim). The files reside in s3://udacity-dend/log_data and looks like this:

```
 {"artist":"Girl Talk","auth":"Logged In","firstName":"Kaylee","gender":"F","itemInSession":8,"lastName":"Summers","length":160.15628,"level":"free","location":"Phoenix-Mesa-Scottsdale, AZ","method":"PUT","page":"NextSong","registration":1540344794796.0,"sessionId":139,"song":"Once again","status":200,"ts":1541107734796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"","userId":"8"}
 ```

 
 ### Data Modeling

- **Fact Table**: 

   - **songplays**: records in log data with song plays(records with page ```NextSong```). Columns: *song_play_id*, *start_time*, *user_id*, *level*, *song_id*, *artist_id*, *session_id*, *location*, *user_agent*.

- **Dimension Tables**:

   - **users**: users in the app. Columns: *user_id*, *first_name*, *last_name*, *gender*, *level*.

   - **songs**: songs in the app. Columns: *song_id*, *artist_id*, *year*, *duration*.

   - **artists**: artists in the app. Columns: *artist_id*, *name*, *location*, *latitude*, *longitude*.

   - **time**: timestamps of records in songplays. Columns: *start_time*, *hour*, *day*, *week*, *month*, *year*, *weekday*.

### Files Description
1. ```dl.cfg``` is a file to configure AWS Credentials to access S3 buckets.

2. ```etl.py ``` contains all ETL processes to load from S3 and write into parquet files to an output location

### Execution Steps
1. Configure and fill all credentials in ```dl.cfg```

2. From inside *data-eng/datalake_spark/* directory, run ```etl.py``` in console:

```
python etl.py
```