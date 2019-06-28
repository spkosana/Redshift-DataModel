import configparser


# CONFIG file gets all the required parameters and their values in the config object 
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES will clean if there are any existing tables with the names 
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS staging_songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE staging tables to load the data from json files from s3 bucket into staging_events 
staging_events_table_create= (""" CREATE TABLE staging_events (
                   artist VARCHAR, 
                   auth VARCHAR,
                   firstname VARCHAR,
                   gender varchar,
                   iteminsession INTEGER,
                   lastname VARCHAR , 
                   length NUMERIC(10,6),
                   level VARCHAR, 
                   location VARCHAR, 
                   method VARCHAR, 
                   page VARCHAR,
                   registration BIGINT, 
                   sessionid INTEGER,
                   song VARCHAR, 
                   status INTEGER, 
                   ts BIGINT, 
                   useragent VARCHAR,
                   userid INTEGER
               );
""")


# staging_songs table is created with columns names based on observation. 

staging_songs_table_create = ( """
               CREATE TABLE staging_songs (
                   artist_id VARCHAR , 
                   artist_latitude NUMERIC(10,5),
                   artist_longitude NUMERIC(10,5),
                   artist_location VARCHAR,
                   artist_name varchar,
                   duration NUMERIC(10,5),
                   num_songs INTEGER , 
                   song_id VARCHAR,
                   title VARCHAR, 
                   year INTEGER
               );
               """)




'''
songplays is the fact table where all the events are being inserted and necessary dimension keys are also inserted to join with dimensions when doing analysis 

'''
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(songplay_id  INTEGER IDENTITY(1,1) PRIMARY KEY,
                                                                     start_time VARCHAR NOT NULL,
                                                                     user_id VARCHAR NOT NULL,
                                                                     level VARCHAR, 
                                                                     song_id VARCHAR NOT NULL, 
                                                                     artist_id VARCHAR NOT NULL, 
                                                                     session_id VARCHAR, 
                                                                     location VARCHAR, 
                                                                     user_agent VARCHAR                                                                 
                                                                     );
""")



'''
dimension table user has all the users in the table. 

'''
user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id INTEGER NOT NULL PRIMARY KEY,
                                                            first_name VARCHAR,
                                                            last_name VARCHAR,
                                                            gender VARCHAR, 
                                                            level VARCHAR
                                                            );
""")



'''
dimension table songs has all the songs in the table. 

'''
song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id VARCHAR NOT NULL PRIMARY KEY,
                                                            title VARCHAR,
                                                            artist_id VARCHAR,
                                                            year INTEGER, 
                                                            duration NUMERIC(10,5)
                                                            );
""")



'''
dimension table artists has all the artists in the table. 

'''
artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id varchar NOT NULL PRIMARY KEY,
                                                              name VARCHAR,
                                                              location VARCHAR,
                                                              latitude NUMERIC(10,5),
                                                              longitude NUMERIC(10,5)
                                                              );
""")



'''
dimension table time has all the time related information in the table. 
'''
time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time timestamp NOT NULL PRIMARY KEY,
                                                           hour int,
                                                           day int, 
                                                           week int,
                                                           month int, 
                                                           year int, 
                                                           weekday int
                                                           );
""")

# Here copy command is used to copy all the data from jsnon files in to STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
                           iam_role  '{}'
                          FORMAT AS JSON {};""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"), config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""COPY staging_songs FROM {}
                         iam_role  '{}' 
                         FORMAT AS JSON 'auto';""").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE","ARN"))



# All the necessary traformations are done on staging tables and finally data is pushed into  FINAL TABLES
songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                              SELECT 
                            TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS START_TIME,
                                    e.userid,
                                    e.level,
                                    s.song_id,
                                    s.artist_id,
                                    e.sessionid,
                                    e.location,
                                    e.useragent
                            FROM dwh.public.staging_events e 
                            join dwh.public.staging_songs  s on e.song = s.title 
                                                                and e.length = s.duration 
                                                                and e.artist = s.artist_name
                            where page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO users(user_id,first_name,last_name,gender,level)
                        SELECT DISTINCT
                        userid,
                        firstName,
                        lastName,
                        gender,
                        level
                        FROM staging_events
                        WHERE userid IS NOT NULL
                        """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT 
                        song_id, 
                        title,
                        artist_id,
                        year,
                        duration
                        FROM staging_songs
                        where song_id is not null
                        """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                        SELECT DISTINCT 
                        artist_id, 
                        artist_name,
                        artist_location,
                        artist_latitude,
                        artist_longitude
                        FROM staging_songs
                        where artist_id is not null
                        """)

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
                        SELECT DISTINCT 
                         TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS START_TIME,
                         EXTRACT(HRS FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS HOUR,
                         EXTRACT(D FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS DAY,
                         EXTRACT(W FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS WEEK,
                         EXTRACT(MON FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS MONTH,
                         EXTRACT(Y FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS YEAR,
                         EXTRACT(DOW FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') weekdaydate
                        FROM staging_events
                        where ts is not null 
""")

# All the query paremeters are placed in the QUERY LISTS and are being called in the etl script which executes corresponding sql statements.

create_table_queries = [staging_events_table_create, staging_songs_table_create,  user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
