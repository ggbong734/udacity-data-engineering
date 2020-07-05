import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType, ShortType, LongType
import pyspark.sql.functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    '''
    Create a Spark Session to access all of Spark functionality
    Returns:
         spark: sparkSession object
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    ''' Extracts song data, creates song and artists table, and uploads them to S3 
    
    Arguments:
        spark {object}: sparkSession object
        input_data {string}: file path for input data on S3
        output_data {string}: file path for output repository on S3
        
    Returns:
        None
    '''
    
    # get filepath to song data file on S3
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # local path for song_data
    #song_data = input_data + 'song_data/song_data/*/*/*/*.json'  # CHANGE FOR S3
    
    # schema of song data
    song_schema = StructType([
        StructField('num_songs', IntegerType()),
        StructField('artist_id', StringType()),
        StructField('artist_latitude', DoubleType()),
        StructField('artist_longitude', DoubleType()),
        StructField('artist_location',StringType()),
        StructField('artist_name', StringType()),
        StructField('song_id', StringType()),
        StructField('title', StringType()),
        StructField('duration', DoubleType()),
        StructField('year', ShortType())
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema = song_schema)

    # extract columns to create songs table and drop duplicates based on song_id
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # dropping rows with duplicate song_id 
    songs_table = songs_table.dropDuplicates(subset=["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(
        output_data + "songs_table",
        mode = "overwrite")
    
    print("Completed writing songs table to s3 bucket")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
    # drop rows with duplicate artist_id
    artists_table = artists_table.dropDuplicates(subset=["artist_id"])
    
    # rename several columns 
    artists_table = artists_table.withColumnRenamed("artist_name", "name")\
                                 .withColumnRenamed("artist_location", "location")\
                                 .withColumnRenamed("artist_latitude", "latitude")\
                                 .withColumnRenamed("artist_longitude", "longitude")
    
    # write artists table to parquet files
    artists_table.write.parquet(
        output_data + "artists_table",
        mode = "overwrite"
    )
    
    print("Completed writing artists table to s3 bucket")


def process_log_data(spark, input_data, output_data):
    ''' Extracts log data, creates time, users, and songplays tables, and uploads them to S3 
    
    Arguments:
        spark {object}: sparkSession object
        input_data {string}: file path for input data on S3
        output_data {string}: file path for output repository on S3
        
    Returns:
        None
    '''
    
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'
    
    # schema of log data
    log_schema = StructType([
        StructField('artist', StringType()),
        StructField('auth', StringType()),
        StructField('firstName', StringType()),
        StructField('gender', StringType()),
        StructField('iteminSession', IntegerType()),
        StructField('lastName', StringType()),
        StructField('length', DoubleType()),
        StructField('level', StringType()),
        StructField('location', StringType()),
        StructField('method', StringType()),
        StructField('page', StringType()),
        StructField('registration', LongType()),
        StructField('sessionId', IntegerType()),
        StructField('song', StringType()),
        StructField('status', ShortType()),
        StructField('ts', LongType()),   # need to convert ts to timestamp using to_timestamp function in spark
        StructField('userAgent', StringType()),
        StructField('userId', IntegerType())
    ])    
    
    # read log data file
    df = spark.read.json(log_data, schema = log_schema)
    
    # filter by actions for song plays
    df = df.where(col("page") == "NextSong")
    
    # rename columns
    df = df.withColumnRenamed("userId", "user_id")\
           .withColumnRenamed("firstName", "first_name")\
           .withColumnRenamed("lastName", "last_name")\
           .withColumnRenamed("sessionId", "session_id")\
           .withColumnRenamed("userAgent", "user_agent")

    # extract columns for users table and drop duplicates based on user_id   
    users_table = df.select("user_id", "first_name", "last_name", "gender", "level")
    
    # drop rows with duplicate user_id
    users_table = users_table.dropDuplicates(subset=["user_id"])
    
    # write users table to parquet files
    users_table.write.parquet(
        output_data + "users_table",
        mode = "overwrite"
    )
    
    print("Completed writing users table to s3 bucket")

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = df.withColumn("timestamp", F.to_timestamp(col("ts").cast(TimestampType())))
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = df.withColumn("datetime", F.to_date(col("ts").cast(TimestampType())))
    
    # create hour, day, week, month, year, weekday column
    df = df.withColumn("hour", hour("timestamp"))\
           .withColumn("day", dayofmonth("datetime"))\
           .withColumn("week", weekofyear("datetime"))\
           .withColumn("month", month("datetime"))\
           .withColumn("year", year("datetime"))\
           .withColumn("weekday", date_format('datetime', 'u'))
    
    # rename column timestamp to start_time
    df = df.withColumnRenamed("timestamp", "start_time")
    
    # extract columns to create time table  
    time_table = df.select("start_time", "hour", "day", "week", "month", "year", "weekday")
    
    # drop rows with duplicate start_time from time table
    time_table = time_table.dropDuplicates(subset=["start_time"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(
        output_data + "time_table",
        mode = "overwrite"
    )
    
    print("Completed writing time table to s3 bucket")

    # read in song data to use for songplays table
    # get filepath to song data file
#     song_data = output_data + 'song_data/song_data/*/*/*/*.json'   # CHANGE for S3
    
#     # schema of song data
#     song_schema = StructType([
#         StructField('num_songs', IntegerType()),
#         StructField('artist_id', StringType()),
#         StructField('artist_latitude', DoubleType()),
#         StructField('artist_longitude', DoubleType()),
#         StructField('artist_location',StringType()),
#         StructField('artist_name', StringType()),
#         StructField('song_id', StringType()),
#         StructField('title', StringType()),
#         StructField('duration', DoubleType()),
#         StructField('year', ShortType())
#     ])
    
    # read songs data file
    song_df = spark.read.parquet(output_data + "songs_table")
    
    # read artists data file 
    artist_df = spark.read.parquet(output_data + "artists_table")
    
    # join song_df and artist_df to get artist name, left join so only join if there is song
    song_artist_df = song_df.join(artist_df, song_df["artist_id"] == artist_df["artist_id"], "left").select(song_df["song_id"],
                                                                                              song_df["title"],
                                                                                              artist_df["name"],
                                                                                              song_df["artist_id"]
                                                                                              )

    # extract columns from joined log df and song_artist_df to create songplays table 
    songplays_table = df.join(song_artist_df, 
                              [
                               df.song == song_artist_df.title,
                               df.artist == song_artist_df.name        
                              ]
                             ).select(df["start_time"],\
                                      df["user_id"],\
                                      df["level"],\
                                      song_artist_df["song_id"],\
                                      song_artist_df["artist_id"],\
                                      df["session_id"],\
                                      df["location"],\
                                      df["user_agent"],\
                                      df["year"],\
                                      df["month"])
    
    # add songplay_id column
    songplays_table = songplays_table.withColumn("songplay_id", F.monotonically_increasing_id())
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(
        output_data + "songplays_table",
        mode = "overwrite"
    )
    
    print("Completed writing songplays table to s3 bucket")


def main():
    ''' Main script which creates the Spark Session, transforms input data and uploads new tables to output repository
    
    Return: None
    
    '''
    spark = create_spark_session()
    
    # Try with local files 
    #input_data = "./data/"     # CHANGE FOR S3
    #output_data = "./output/"  # CHANGE FOR S3
    
    # input and output file paths on S3
    input_data = "s3a://udacity-dend/"    
    output_data = "s3a://dend-bucket-s3-1234/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    print("Completed all processing for the ETL pipeline")
    
if __name__ == "__main__":
    main()
