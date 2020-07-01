import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, Row, Window
from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import IntegerType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Creates SparkSession and returns it
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Processing song_data 
        
    Processes song_data from S3 bucket.
    Creates tables "songs" and "artists"
    
    Params:
        spark: SparkSession
        input_data: input json files
        output_data: output directory to save files in .parquet
    """
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates(["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data+'songs', 'overwrite')


    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]).dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'artists', 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
        Processing log_data 
        
    Processes log_data from S3 bucket.
    Creates tables "users", "time" and "songplays"
    
    Params:
        spark: SparkSession
        input_data: input json files
        output_data: output directory to save files in .parquet
    """
    # get filepath to log data file
    log_data =os.path.join(input_data,"log_data/*/*/*.json")
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=='NextSong')

    # extract columns for users table    
    users_table = df.select(["userId", "firstName", "lastName", "gender", "level"]).dropDuplicates(["userId"])
    
    # write users table to parquet files
    users_table.write.parquet(output_data+'users', 'overwrite')

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
                    .select("start_time", "hour", "day", "week", "month", "year", "weekday").dropDuplicates(["start_time"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data+'time', 'overwrite')

    # read in song asn artist data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs/')
    song_df = song_df.withColumnRenamed("artist_id", "song_artist_id")
    artists_df = spark.read.parquet(output_data+'artists/')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (song_df.title == df.song)).join(artists_df, (artists_df.artist_name == df.artist))

    # setting id to songplays table
    window = Window.orderBy(col('sessionId'))
    songplays_table = songplays_table.withColumn('songplay_id', row_number().over(window))
    songplays_table = songplays_table.select(["songplay_id", "start_time", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent"])

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.withColumn("year", year("start_time"))\
                                     .withColumn("month", month("start_time"))
    songplays_table.write.partitionBy('year', 'month').parquet(output_data+'songplays', 'overwrite')

def main():
    """
        Call functions to process data
    """
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    output_data = "output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
