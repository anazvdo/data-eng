from datetime import datetime, timedelta
import os
from airflow.models import Variable
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')
redshift_conn_id = Variable.get('redshift_conn_id')

default_args = {
    'owner': 'Ana Caroline Reis',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': True
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id=redshift_conn_id,
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    table='public.staging_events',
    s3_path='s3://udacity-dend/log_data',
    region='us-west-2',
    json_meta='s3://udacity-dend/log_json_path.json',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id=redshift_conn_id,
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    table='public.staging_songs',
    s3_path='s3://udacity-dend/song_data/',
    region='us-west-2',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id=redshift_conn_id,
    table='public.songplays',
    query='''
        SELECT DISTINCT DATE_ADD('ms', se.ts, '1970-01-01') AS start_time, 
            se.userid,
            se.level,
            ss.song_id,
            ss.artist_id,
            se.sessionid,
            se.location,
            se.useragent
        FROM staging_events se
        JOIN staging_songs ss
            ON se.song = ss.title
            AND se.artist = ss.artist_name
        WHERE se.page = 'NextSong'
        AND se.userid IS NOT NULL
    ''',
    columns='start_time, userid, level, songid, artistid, sessionid, location, user_agent',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id=redshift_conn_id,
    table='users',
    query='''
    SELECT DISTINCT
        userid,
        firstname,
        lastname,
        gender,
        level
    FROM staging_events
    WHERE page='NextSong'
    ''',
    append=False,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id=redshift_conn_id,
    table='songs',
    query='''
    SELECT 
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    ''',
    append=False,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id=redshift_conn_id,
    table='artists',
    query='''
    SELECT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    ''',
    append=False,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id=redshift_conn_id,
    table='time',
    query='''
    SELECT DISTINCT
        time_table.start_time,
        EXTRACT (HOUR FROM time_table.start_time), 
        EXTRACT (DAY FROM time_table.start_time),
        EXTRACT (WEEK FROM time_table.start_time), 
        EXTRACT (MONTH FROM time_table.start_time),
        EXTRACT (YEAR FROM time_table.start_time), 
        EXTRACT (WEEKDAY FROM time_table.start_time)
    FROM (SELECT timestamp 'epoch' + ts/1000 *INTERVAL '1 second' as start_time 
          FROM staging_events) as time_table
    ''',
    append=False,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id=redshift_conn_id,
    data_quality_checks=[{'sql': "SELECT COUNT(*) FROM users", 'not_expected': 0},
                         {'sql': "SELECT COUNT(*) FROM songs", 'not_expected': 0},
                         {'sql': "SELECT COUNT(*) FROM artists", 'not_expected': 0},
                         {'sql': "SELECT COUNT(*) FROM songplays", 'not_expected': 0},
                        ],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Dependencies
start_operator >> [stage_events_to_redshift, 
                   stage_songs_to_redshift] >> \
load_songplays_table >> [load_song_dimension_table,
                         load_user_dimension_table,
                         load_artist_dimension_table,
                         load_time_dimension_table] >> \
run_quality_checks >> end_operator