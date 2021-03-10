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
    event_id INT IDENTITY(0,1) PRIMARY KEY,
    artist VARCHAR(max),
    auth VARCHAR(max),
    firstName VARCHAR(max),
    gender CHAR(1),
    itemInSession INT,
    lastName VARCHAR(max),
    length DOUBLE PRECISION,
    level VARCHAR(max),
    location VARCHAR(max),
    method VARCHAR(max),
    page VARCHAR(300),
    registration DOUBLE PRECISION,
    sessionId INT,
    song VARCHAR(max),
    status INT,
    ts BIGINT,
    userAgent VARCHAR(max),
    userId VARCHAR(max));
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    artist_id VARCHAR(max), 
    artist_latitude DOUBLE PRECISION,
    artist_location VARCHAR(max),
    artist_longitude DOUBLE PRECISION,
    artist_name VARCHAR(max),
    duration DOUBLE PRECISION,
    num_songs INT,
    song_id VARCHAR(max),
    title VARCHAR(max),
    year INT4
    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int IDENTITY(0,1) PRIMARY KEY, 
    start_time TIMESTAMP, 
    user_id VARCHAR(max) NOT NULL, 
    level VARCHAR(max), 
    song_id VARCHAR(max) NOT NULL, 
    artist_id VARCHAR(max) NOT NULL, 
    session_id BIGINT NOT NULL, 
    location VARCHAR(max), 
    user_agent VARCHAR(max)
    --FOREIGN KEY (start_time) REFERENCES time(start_time),
    --FOREIGN KEY (user_id) REFERENCES users(user_id),
    --FOREIGN KEY (song_id) REFERENCES songs(song_id),
    --FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
    )
    DISTKEY(song_id)
    SORTKEY(start_time, session_id);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id VARCHAR(max) PRIMARY KEY,
    first_name VARCHAR(max),
    last_name VARCHAR(max),
    gender CHAR(1),
    level VARCHAR(max) NOT NULL);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR(max) PRIMARY KEY,
    title VARCHAR(max),
    artist_id VARCHAR(max) NOT NULL,
    year INT4,
    duration DOUBLE PRECISION)
    DISTKEY(artist_id)
    SORTKEY(song_id, year);
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id VARCHAR(max) PRIMARY KEY,
    name VARCHAR(max),
    location VARCHAR(max),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION);
""")

time_table_create = ("""
CREATE TABLE time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT4,
    weekday INT);
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

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
