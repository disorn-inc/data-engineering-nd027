# Drop Tables

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
users_table_drop = "DROP TABLE IF EXISTS users"
songs_table_drop = "DROP TABLE IF EXISTS songs"
artists_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF NOT EXISTS time"

# Create Fact Table
create_songplay_table = ("""CREATE TABLE IF NOT EXISTS songplays (
                            songplay_id serial PRIMARY KEY NOT NUL, 
                            start_time varchar REFERENCES time(start_time), 
                            user_id int REFERENCES users(user_id),
                            level varchar,
                            song_id varchar REFERENCES songs(song_id), 
                            artist_id varchar REFERENCES artists(artist_id),
                            session_id varchar,
                            location varchar, 
                            varchar user_agent)
                """)

# Create Dimension Tables
create_users_table = ("""CREATE TABLE IF NOT EXISTS users(
                         user_id int PRIMARY KEY,
                         first_name varchar, 
                         last_name varchar, 
                         gender varchar, 
                         level varchar)
                """)

create_songs_table = ("""CREATE TABLE IF NOT EXISTS songs(
                         song_id varchar PRIMARY KEY, 
                         title varchar, 
                         artist_id varchar NOT NULL, 
                         year int, 
                         duration float)
                """)

create_artists_table = ("""CREATE TABLE IF NOT EXISTS artists(
                           artist_id varchar PRIMARY KEY, 
                           name varchar, 
                           location varchar, 
                           latitude varchar, 
                           longitude varchar)
                """)

create_time_table = ("""CREATE TABLE IF NOT EXISTS time(
                        start_time varchar PRIMARY KEY, 
                        hour int, 
                        day int, 
                        week int, 
                        month int, 
                        year int, 
                        weekday int)
                """)

# Insert into Tables
insert_songplay_table = ("""INSERT INTO songplays (
                            start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
                            VALUES(%s, %s, %s, %s ,%s, %s, %s ,%s)
                            ON CONFLICT (songplay_id) DO NOTHING;
                """)

insert_users_table = ("""INSERT INTO users (
                         user_id, first_name, last_name, gender, level)
                         VALUES (%s, %s, %s, %s ,%s)
                         ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level
                """)

insert_songs_table = ("""INSERT INTO songs (
                         song_id, title, artist_id, year, duration) 
                         VALUES (%s, %s, %s, %s ,%s)
                         ON CONFLICT (song_id) DO NOTHING;
                """)

insert_artists_table = ("""INSERT INTO artists (
                           artist_id, name, location, latitude, longitude) 
                           VALUES (%s, %s, %s, %s ,%s)
                           ON CONFLICT (artist_id) DO NOTHING;
                """)

insert_time_table = ("""INSERT INTO time (
                        start_time, hour, day, week, month, year, weekday)
                        VALUES (%s, %s, %s, %s ,%s ,%s , %s)
                        ON CONFLICT (start_time) DO NOTHING;
                """)

# Find Songs
"""The analytics team is particularly interested in 
understanding what songs users are listening to."""

song_select = ("""
                    SELECT s.song_id, s.artist_id
                    FROM songs AS s
                    JOIN artists AS a ON a.artist_id = s.artist_id
                    WHERE s.title = %s
                    AND a.name = %s
                    AND s.duration = %s
                """)
