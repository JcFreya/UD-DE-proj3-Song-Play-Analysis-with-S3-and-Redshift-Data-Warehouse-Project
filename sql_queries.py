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
    event_id integer IDENTITY(0,1) PRIMARY KEY,
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession integer,
    lastName varchar,
    length double precision,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration double precision,
    sessionId integer,
    song varchar,
    status integer,
    ts bigint,
    userAgent varchar,
    userId varchar);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    artist_id varchar, 
    artist_latitude double precision,
    artist_location varchar,
    artist_longitude double precision,
    artist_name varchar,
    duration double precision,
    num_songs integer,
    song_id varchar,
    title varchar,
    year integer
    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int IDENTITY(0,1) PRIMARY KEY NOT NULL, 
    start_time timestamp NOT NULL, 
    user_id varchar NOT NULL, 
    level varchar, 
    song_id varchar NOT NULL, 
    artist_id varchar, 
    session_id BIGINT NOT NULL, 
    location varchar, 
    user_agent varchar);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id varchar PRIMARY KEY NOT NULL,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar NOT NULL);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id varchar PRIMARY KEY NOT NULL,
    title varchar,
    artist_id varchar NOT NULL,
    year integer,
    duration double precision);
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id varchar PRIMARY KEY NOT NULL,
    name varchar,
    location varchar,
    latitude double precision,
    longitude double precision);
""")

time_table_create = ("""
CREATE TABLE time(
    start_time timestamp PRIMARY KEY NOT NULL,
    hour integer,
    day integer,
    week integer,
    month integer,
    year integer,
    weekday integer);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events
from {}
iam_role {}
json {};
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs
from {}
iam_role {}
json 'auto';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT  
    TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time, 
    e.userId, 
    e.level, 
    s.song_id as song_id,
    s.artist_id, 
    e.sessionId as session_id,
    e.location as location, 
    e.userAgent as user_agent
FROM staging_events e, staging_songs s
WHERE e.page = 'NextSong' 
AND e.song = s.title 
AND e.artist = s.artist_name 
AND e.length = s.duration
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT  
    userId, 
    firstName, 
    lastName, 
    gender, 
    level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT DISTINCT 
    song_id, 
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
SELECT DISTINCT 
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT start_time, 
    extract(hour from start_time) as hour,
    extract(day from start_time) as day,
    extract(week from start_time) as week, 
    extract(month from start_time) as month,
    extract(year from start_time)as year, 
    extract(dayofweek from start_time) as weekday
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
