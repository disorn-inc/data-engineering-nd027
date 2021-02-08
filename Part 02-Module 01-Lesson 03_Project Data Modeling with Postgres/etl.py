import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    """Do ETL on the song_data dataset to create the songs and artists table"""

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(insert_songs_table, song_data)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
    cur.execute(insert_artists_table, artist_data)