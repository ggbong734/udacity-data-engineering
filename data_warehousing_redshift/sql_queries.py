import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE_ARN = config.get("IAM_ROLE","ARN")

LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")

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
CREATE TABLE IF NOT EXISTS staging_events
    (event_id BIGINT IDENTITY(0,1),
     artist VARCHAR,
     auth VARCHAR(30),
     firstName VARCHAR(30),
     gender CHAR(3),
     iteminSession INTEGER,
     lastName VARCHAR(30),
     length DECIMAL,
     level CHAR(10),
     location VARCHAR(100),
     method VARCHAR(10),
     page VARCHAR(20),
     registration VARCHAR,
     sessionId INTEGER DISTKEY SORTKEY,
     song VARCHAR,
     status INTEGER,
     ts BIGINT,
     userAgent VARCHAR,
     userId VARCHAR(50));
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
    (num_songs INTEGER,
     artist_id VARCHAR(50),
     artist_latitude DECIMAL,
     artist_longitude DECIMAL,
     artist_location VARCHAR,
     artist_name VARCHAR,
     song_id VARCHAR(50) DISTKEY SORTKEY,
     title VARCHAR,
     duration DECIMAL,
     year INTEGER);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
    (user_id VARCHAR PRIMARY KEY,
     first_name VARCHAR(30),
     last_name VARCHAR(30),
     gender CHAR(3),
     level CHAR(10));
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
    (song_id VARCHAR(50) PRIMARY KEY,
     title VARCHAR,
     artist_id VARCHAR(50),
     year INTEGER,
     duration DECIMAL);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
    (artist_id VARCHAR(50) PRIMARY KEY,
     name VARCHAR,
     location VARCHAR,
     latitude DECIMAL,
     longitude DECIMAL);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
    (start_time TIMESTAMP PRIMARY KEY,
     hour SMALLINT,
     day SMALLINT,
     week SMALLINT,
     month SMALLINT,
     year SMALLINT,
     weekday SMALLINT);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
    (songplay_id INTEGER IDENTITY (0,1) PRIMARY KEY,
     start_time TIMESTAMP, 
     user_id VARCHAR(50), 
     level VARCHAR(20),
     song_id VARCHAR(50), 
     artist_id VARCHAR(50), 
     session_id INTEGER,
     location VARCHAR(100),
     user_agent VARCHAR
     );
""")

# FOREIGN KEY(start_time) REFERENCES time(start_time),
# FOREIGN KEY(user_id) REFERENCES users(user_id),
# FOREIGN KEY(song_id) REFERENCES songs(song_id),
# FOREIGN KEY(artist_id) REFERENCES artists(artist_id)

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
IAM_ROLE {}
REGION 'us-west-2'
FORMAT AS JSON {};
""").format(LOG_DATA, IAM_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
IAM_ROLE {}
REGION 'us-west-2'
FORMAT AS JSON 'auto'
ACCEPTINVCHARS AS '*';
""").format(SONG_DATA, IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' AS start_time,
           se.userId AS user_id,
           se.level,
           ss.song_id,
           ss.artist_id,
           se.sessionId AS session_id,
           se.location,
           se.userAgent AS user_agent
    FROM staging_events AS se
    LEFT JOIN staging_songs AS ss
    ON se.song = ss.title
""")

# Redshift does not support UPSERT statements like ON CONFLICT DO/UPDATE
# So in place of ON CONFLICT we use DISTINCT

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId AS user_id, 
           firstName AS first_name, 
           lastName AS last_name, 
           gender, 
           level 
    FROM staging_events;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, 
           title, 
           artist_id, 
           year, 
           duration
    FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, 
           artist_name AS name, 
           artist_location AS location, 
           artist_latitude AS latitude, 
           artist_longitude AS longitude
    FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT start_time,
        EXTRACT(hour from start_time) AS hour,
        EXTRACT(day from start_time) AS day,
        EXTRACT(week from start_time) AS week,
        EXTRACT(month from start_time) AS month,
        EXTRACT(year from start_time) AS year,
        EXTRACT(dayofweek from start_time) AS weekday
    FROM (SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS start_time
            FROM staging_events) AS time_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
