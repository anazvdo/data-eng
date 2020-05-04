import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
      Processes an entire song file, puts this into a dataframe to select 
      needed columns and insert rows into song and artist tables.

      'cur': psycopg2 cursor from sparkifydb
      'filepath': string with filepath of song file      
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = artist_data.values[0].tolist()    
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]  
    song_data = song_data.values[0].tolist()  
    cur.execute(song_table_insert, song_data)
    

def process_log_file(cur, filepath):
    """
      Processes an entire log file, puts this into a dataframe to select 
      needed columns, filters pages with 'NextSong', transforms timestamps 
      into datetime values and insert rows into time, user and songplay tables.

      'cur': psycopg2 cursor from sparkifydb
      'filepath': string with filepath of log file      
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    # convert timestamp column to datetime
    t = df['ts']
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.dayofweek]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None        

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
        Gets all files by walking through directories(given an initial filepath) 
        and call a specific function to read the data inside.

        'cur': psycopg2 cursor from sparkifydb
        'conn': psycopg2 connection to sparkifydb
        'filepath': string with filepath of song or log file
        'func': function to be called to process and read data from file
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
        Using sparkifydb connection, the ETL process is done here:
        - Process data from song files to be transformed and loaded into database
        - Process data from log files to be transformed and loaded into database
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()