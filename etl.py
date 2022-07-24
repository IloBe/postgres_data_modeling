#!/usr/bin/env python3

"""
This ETL script reads and processes files from song_data and log_data and loads them into the tables.
It is filled out based on the work of the ETL Jupyter notebook.
"""

##############################
# Imports
##############################
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Processes a song file from the song_data directory and
    store its record data in the song and artists tables.
    
    Input:
        cur (cursor): cursor of Postgres Db connection
        filepath (string): filepath of song_data dir about songs and artists
    Output:
        None
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Processes a log file from the log_data directory and
    store its record data in the time and users dimensional tables.
    Additionally, creates the songplays fact table.
    
    Input:
        cur (cursor): cursor of Postgres DB connection
        filepath (string): filepath of log_data dir about time and users
    Output:
        None
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    # the songplays table includes the timestamp in its start_time column
    time_data = ([item, item.hour, item.day, item.week, item.month, item.year, item.dayofweek] for item in t)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(data=time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    # note: duplicates exists in the original dataframe, we remove them having only one row for each user
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()

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
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Iterator function to process all data for Postgres database storage.
    
    Input:
        cur (cursor): cursor of Postgres Db connection
        conn (object): Postgres Db connection object
        filepath (string): filepath to the data directories (song_data, log_data)
        func: function to be executed (process_song_file, process_log_file)
    Output:
        Prints out the total number of files found together with the iteration status number.
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{num_files} files found in {filepath}')

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        print(f'{i}/{num_files} files processed.')


def main():
    """
    Main ETL workflow setting for Postgres Db connection and cursor.
    Auto commit is configured to be True.
    Furthermore, triggers the processing of the files included in the directories song_data and log_data.
    Finally, the Db connection is closed.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    conn.set_session(autocommit=True)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()