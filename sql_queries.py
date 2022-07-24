#!/usr/bin/env python3

"""
Script that contains all the sql queries. It is imported into the following files:
- create_tables.py
- etl.ipynb
- etl.py

Note:
Data modeling focus is the songplay list of a user.
In general, a song does not exist without an artist.

During test run with artist_id and/or song_id being NOT NULL issues appeared, e.g.:
'IntegrityError: null value in column "song_id" violates not-null constraint'.
In other words, an artist can exist without a song. So, song_id can be NULL.
Furthermore, artist_id can be null as well, if a user exists without a songplay list,
but if an artist is available the person has a name.

As a final insight, the user_id must be not null, but both song and artist ids can be.
If a user_id conflict occurs and having a look to our ER diagram of the users table,
we use the virtual EXCLUDED table to update the level column attribute on a new value.
This kind of handling with the virtual EXCLUDED table is used as well for some other
column attributes which shall not be null (see test notebook and its sanity checks).
Additionally, all this belongs to a specific time information.

For more information how to handle conflicts see e.g.:
https://www.prisma.io/dataguide/postgresql/inserting-and-modifying-data/insert-on-conflict
"""

##############################
# Coding
##############################

#
# DROP TABLES
#
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

#
# CREATE TABLES
# add NOT NULL info for relevant attributes
#
try:
    songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays ( \
                                    songplay_id SERIAL PRIMARY KEY, \
                                    start_time timestamp NOT NULL, \
                                    user_id int NOT NULL, \
                                    level varchar, \
                                    song_id varchar, \
                                    artist_id varchar, \
                                    session_id int, \
                                    location varchar, \
                                    user_agent varchar \
                             );""")
except psycopg2.Error as e: 
    print("Error: Issue creating songplay table")
    print (e)

try:
    user_table_create = ("""CREATE TABLE IF NOT EXISTS users( \
                                    user_id int PRIMARY KEY NOT NULL, \
                                    first_name varchar, \
                                    last_name varchar, \
                                    gender varchar, \
                                    level varchar \
                         );""")
except psycopg2.Error as e: 
    print("Error: Issue creating user table")
    print (e)

try:
    song_table_create = ("""CREATE TABLE IF NOT EXISTS songs( \
                                    song_id varchar PRIMARY KEY, \
                                    title varchar NOT NULL, \
                                    artist_id varchar, \
                                    year int, \
                                    duration float NOT NULL\
                         );""")
except psycopg2.Error as e: 
    print("Error: Issue creating song table")
    print (e)

try:
    artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists ( \
                                    artist_id varchar PRIMARY KEY, \
                                    name varchar NOT NULL, \
                                    location varchar, \
                                    latitude float, \
                                    longitude float \
                           );""")
except psycopg2.Error as e: 
    print("Error: Issue creating artist table")
    print (e)

try:
    time_table_create = ("""CREATE TABLE IF NOT EXISTS time ( \
                                    start_time timestamp NOT NULL, \
                                    hour int, \
                                    day int, \
                                    week int, \
                                    month int, \
                                    year int, \
                                    weekday int \
                         );""")
except psycopg2.Error as e: 
    print("Error: Issue creating time table")
    print (e)

# 
# INSERT RECORDS
# take care of conflicts during insertion of rows
#
try:
    songplay_table_insert = ("""INSERT INTO songplays ( \
                                    start_time,  \
                                    user_id,  \
                                    level,  \
                                    song_id,  \
                                    artist_id,  \
                                    session_id,  \
                                    location,  \
                                    user_agent) \
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) \
                                ON CONFLICT DO NOTHING; \
                             """)
except psycopg2.Error as e: 
    print("Error: Inserting songplay rows")
    print (e)

try:
    user_table_insert = ("""INSERT INTO users ( \
                                user_id,  \
                                first_name,  \
                                last_name,  \
                                gender,  \
                                level) \
                            VALUES (%s, %s, %s, %s, %s) \
                            ON CONFLICT (user_id) DO UPDATE \
                            SET level = EXCLUDED.level; \
                         """)
except psycopg2.Error as e: 
    print("Error: Inserting user rows")
    print (e)

try:
    song_table_insert = ("""INSERT INTO songs ( \
                                song_id,  \
                                title,  \
                                artist_id,  \
                                year, \
                                duration) \
                            VALUES (%s, %s, %s, %s, %s) \
                            ON CONFLICT (song_id) DO NOTHING; \
                         """)
except psycopg2.Error as e: 
    print("Error: Inserting song rows")
    print (e)

try:
    artist_table_insert = ("""INSERT INTO artists ( \
                                artist_id,  \
                                name,  \
                                location,  \
                                latitude,  \
                                longitude) \
                              VALUES (%s, %s, %s, %s, %s) \
                              ON CONFLICT (artist_id) DO UPDATE \
                              SET name = EXCLUDED.name
                              ; \
                           """)
except psycopg2.Error as e: 
    print("Error: Inserting artist rows")
    print (e)

try:
    time_table_insert = ("""INSERT INTO time ( \
                                start_time, \
                                hour, \
                                day, \
                                week,  \
                                month,  \
                                year,  \
                                weekday) \
                            VALUES (%s, %s, %s, %s, %s, %s, %s); \
                         """)
except psycopg2.Error as e: 
    print("Error: Inserting time rows")
    print (e)

# 
# FIND SONGS
#
try:
    song_select = ("""SELECT songs.song_id, songs.artist_id \
                      FROM songs \
                      JOIN artists ON songs.artist_id = artists.artist_id \
                      WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s; \
                   """)
except psycopg2.Error as e: 
    print("Error: Selecting a song")
    print (e)

#
# QUERY LISTS
#
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
