## Data Modeling with Cassandra - Sparkify 

### Introduction 

<i>Sparkify</i> is a startup that owns a new music streaming app.
They've collected some data about songs and user activity on this app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They need to create an Apache Cassandra database which can create queries on song play data to answer the questions.

### Dataset

The dataset is inside <code>event_data</code> directory partitioned by date.

### Project Template

The file named *Project_1B_ Project_Template.ipynb* is created with ETL process in this project.

**Part I** aggregates all csv files into one single csv file(denormalized dataset).

**Part II** creates tables, inserts into them and selects information from them. Tables were created based in pre-defined queries.


### Project Steps - Apache Cassandra

1. CREATE and SET KEYSPACE *sparkify*

2. CREATE TABLE to answer Query 1: *artist_and_song*
    - INSERT INTO TABLE *artist_and_song*
    - SELECT the respective query

3. CREATE TABLE to answer Query 2: *song_and_user*
    - INSERT INTO TABLE *song_and_user*
    - SELECT the respective query

4. CREATE TABLE to answer Query 3: *user_by_song*
    - INSERT INTO TABLE *user_by_song*
    - SELECT the respective query

5. DROP TABLES

6. Close the session

