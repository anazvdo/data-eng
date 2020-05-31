import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

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
            artist varchar,
            auth varchar,
            firstName varchar,
            gender varchar,
            iteminSession int,
            lastname varchar,
            length double precision,
            level varchar,
            location varchar,
            method varchar,
            page varchar,
            registration bigint,
            sessionId int,
            song varchar,
            status int,
            ts bigint,
            userAgent varchar,
            userId int
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
            artist_id varchar,
            artist_latitude double precision,
            artist_location varchar,
            artist_longitude double precision,
            artist_name varchar,
            duration double precision,
            num_songs int,
            song_id varchar,
            title varchar,
            year int
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id int IDENTITY(0,1) PRIMARY KEY NOT NULL,
        start_time timestamp NOT NULL REFERENCES time(start_time),
        user_id int NOT NULL REFERENCES users(user_id),
        level varchar,
        song_id varchar NOT NULL REFERENCES songs(song_id),
        artist_id varchar NOT NULL REFERENCES artists(artist_id),
        session_id int,
        location varchar,
        user_agent varchar
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id int PRIMARY KEY NOT NULL,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id varchar PRIMARY KEY NOT NULL,
        title varchar,
        artist_id varchar NOT NULL REFERENCES artists(artist_id),
        year int,
        duration double precision
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar PRIMARY KEY NOT NULL,
        name varchar,
        location varchar,
        latitude double precision,
        longitude double precision   
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time timestamp PRIMARY KEY NOT NULL,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday varchar
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    REGION 'us-west-2'
    COMPUPDATE OFF
    JSON {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    REGION 'us-west-2'
    COMPUPDATE OFF
    JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(SELECT DISTINCT DATE_ADD('ms', se.ts, '1970-01-01') AS start_time, 
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
);
""")

user_table_insert = ("""
INSERT INTO users(
    SELECT DISTINCT
        userid,
        firstname,
        lastname,
        gender,
        level
    FROM staging_events
    WHERE page='NextSong'
);
""")

song_table_insert = ("""
INSERT INTO songs(
    SELECT 
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
);
""")

artist_table_insert = ("""
INSERT INTO artists(
    SELECT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
);
""")

time_table_insert = ("""
INSERT INTO time(
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
);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
