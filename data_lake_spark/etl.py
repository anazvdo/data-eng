import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join('output/', 'songs'), 'overwrite') #change output


    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join('output/', 'artists'), 'overwrite') #change output


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data+'/*.json'

    # read log data file
    df = spark.read.json(input_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=='NextSong')

    # extract columns for users table    
    users_table = df.select(["userId", "firstName", "lastName", "gender", "level"])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join('output/', 'users'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:  datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table =  df.withColumn("hour", hour("start_time"))\
                    .withColumn("day", dayofmonth("start_time"))\
                    .withColumn("week", weekofyear("start_time"))\
                    .withColumn("month", month("start_time"))\
                    .withColumn("year", year("start_time"))\
                    .withColumn("weekday", dayofweek("start_time"))\
                    .select("start_time", "hour", "day", "week", "month", "year", "weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join('output/', 'time'), 'overwrite') #change output

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs/')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
