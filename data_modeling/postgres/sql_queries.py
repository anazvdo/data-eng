# DROP TABLES

songplay_table_drop = "DROP TABLE songplays"
user_table_drop = "DROP TABLE users"
song_table_drop = "DROP TABLE songs"
artist_table_drop = "DROP TABLE artists"
time_table_drop = "DROP TABLE time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id varchar PRIMARY KEY NOT NULL,
        start_time timestamp,
        user_id int REFERENCES users(user_id),
        level varchar,
        song_id varchar REFERENCES songs(song_id),
        artist_id varchar REFERENCES artists(artist_id),
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
        artist_id varchar REFERENCES artists(artist_id),
        year int,
        duration double
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar PRIMARY KEY,
        name varchar,
        location varchar,
        latitude double,
        longitude double
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time datetime PRIMARY KEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday varchar
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")


time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]