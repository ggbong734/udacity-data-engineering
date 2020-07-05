import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    - Open song file using filepath
    - Insert song record into songs table
    - Insert artist record into artists table
    
    :param cur: cursor in PostGreSQL database
    :param filepath: string representing song file location
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    - Open log file using filepath
    - Extract time data and insert time record into time table
    - Extract user data and insert into user table
    - Extract songplay data anad insert into songplays table
    
    :param cur: cursor in PostGreSQL database
    :param filepath: string representing log file location
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # creating a new column of formatted converted timestamp
    df['ts_formatted'] = t 
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.dayofweek]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    
    # create a dictionary of column_label: time_data 
    d = {}
    for i in range(len(time_data)):
        d[column_labels[i]] = time_data[i]
    
    # create a DataFrame using the dictionary
    time_df = pd.DataFrame(d)

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
        songplay_data = (row.ts_formatted, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    - Obtain all .json files at the specified directory
    - Obtain number of files found
    - Apply func on all of the files
    
    :param cur: cursor in PostGreSQL database
    :param conn: connection to PostGreSQL database
    :param filepath: string representing directory location
    :param func: function to apply to each .json file
    '''
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
    '''
    - Connect to PostGresSQL database
    - Create a cursor
    - Process song and log data files
    - Close connection to database
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()