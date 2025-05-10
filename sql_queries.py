import configparser

# ======================
# LOAD CONFIGURATION
# ======================
config = configparser.ConfigParser()
config.read('dwh.cfg', encoding='utf-8')

# Extract S3 and IAM parameters
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE_ARN = config.get("IAM_ROLE", "IAM_ROLE_ARN")

# ======================
# DROP TABLE STATEMENTS
# ======================
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# ======================
# CREATE TABLE STATEMENTS
# ======================

# Staging tables
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist          TEXT,
    auth            TEXT,
    firstName       TEXT,
    gender          TEXT,
    itemInSession   INT,
    lastName        TEXT,
    length          FLOAT,
    level           TEXT,
    location        TEXT,
    method          TEXT,
    page            TEXT,
    registration    BIGINT,
    sessionId       INT,
    song            TEXT,
    status          INT,
    ts              BIGINT,
    userAgent       TEXT,
    userId          INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs        INT,
    artist_id        TEXT,
    artist_latitude  FLOAT,
    artist_longitude FLOAT,
    artist_location  TEXT,
    artist_name      TEXT,
    song_id          TEXT,
    title            TEXT,
    duration         FLOAT,
    year             INT
);
""")

# Fact table
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time  TIMESTAMP NOT NULL,
    user_id     INT NOT NULL,
    level       TEXT,
    song_id     TEXT,
    artist_id   TEXT,
    session_id  INT,
    location    TEXT,
    user_agent  TEXT
);
""")

# Dimension tables
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id    INT PRIMARY KEY,
    first_name TEXT,
    last_name  TEXT,
    gender     TEXT,
    level      TEXT
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id   TEXT PRIMARY KEY,
    title     TEXT,
    artist_id TEXT,
    year      INT,
    duration  FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT PRIMARY KEY,
    name      TEXT,
    location  TEXT,
    latitude  FLOAT,
    longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour       INT,
    day        INT,
    week       INT,
    month      INT,
    year       INT,
    weekday    INT
);
""")

# ======================
# COPY DATA TO STAGING
# ======================
staging_events_copy = (f"""
COPY staging_events FROM '{LOG_DATA}'
CREDENTIALS 'aws_iam_role={IAM_ROLE_ARN}'
REGION 'us-west-2'
FORMAT AS JSON '{LOG_JSONPATH}';
""")

staging_songs_copy = (f"""
COPY staging_songs FROM '{SONG_DATA}'
CREDENTIALS 'aws_iam_role={IAM_ROLE_ARN}'
REGION 'us-west-2'
FORMAT AS JSON 'auto';
""")

# ======================
# INSERT INTO FINAL TABLES
# ======================

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    userId     AS user_id,
    firstName  AS first_name,
    lastName   AS last_name,
    gender,
    level
FROM staging_events
WHERE userId IS NOT NULL;
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
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name      AS name,
    artist_location  AS location,
    artist_latitude  AS latitude,
    artist_longitude AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
    EXTRACT(hour FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'),
    EXTRACT(day FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'),
    EXTRACT(week FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'),
    EXTRACT(month FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'),
    EXTRACT(year FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'),
    EXTRACT(weekday FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')
FROM staging_events
WHERE ts IS NOT NULL;
""")

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,
    e.userId       AS user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionId    AS session_id,
    e.location,
    e.userAgent    AS user_agent
FROM staging_events e
JOIN staging_songs s
  ON e.song = s.title AND e.artist = s.artist_name
WHERE e.page = 'NextSong';
""")

# ======================
# QUERY LISTS
# ======================

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
    songplay_table_insert
]













# import configparser


# # CONFIG
# config = configparser.ConfigParser()
# config.read('dwh.cfg')

# # DROP TABLES

# staging_events_table_drop = ""
# staging_songs_table_drop = ""
# songplay_table_drop = ""
# user_table_drop = ""
# song_table_drop = ""
# artist_table_drop = ""
# time_table_drop = ""

# # CREATE TABLES

# staging_events_table_create= ("""
# """)

# staging_songs_table_create = ("""
# """)

# songplay_table_create = ("""
# """)

# user_table_create = ("""
# """)

# song_table_create = ("""
# """)

# artist_table_create = ("""
# """)

# time_table_create = ("""
# """)

# # STAGING TABLES

# staging_events_copy = ("""
# """).format()

# staging_songs_copy = ("""
# """).format()

# # FINAL TABLES

# songplay_table_insert = ("""
# """)

# user_table_insert = ("""
# """)

# song_table_insert = ("""
# """)

# artist_table_insert = ("""
# """)

# time_table_insert = ("""
# """)

# # QUERY LISTS

# create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
# copy_table_queries = [staging_events_copy, staging_songs_copy]
# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
