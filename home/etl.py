import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,to_timestamp,dayofweek,from_unixtime
import pyspark.sql.functions as f


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    get filepath to song data file, this will be accessing the public s3 bucket 
    of udacity that we specified in the spark session config to connect to
    """   

    song_data = os.path.join(input_data, "song_data/*/*/*/*.json" )
        
    # read song data file from public Udacity S3 Bucket
    song_data_df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_data_df.select('artist_id', 'duration', 'song_id', 'title', 'year').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist, 
    # used overwrite to replace any songs table if it was in there or create it if it doesn't exist 
    output_songs_table = songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "output-songs", "overwrite")

    # extract columns to create artists table
    artists_table = song_data_df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_latitude').dropDuplicates()
    
    # write artists table to parquet files, used overwrite so it replaces the data f they were there or just put it f nothing is there
    output_artists_table = artists_table.write.parquet(output_data + "output-artists", "overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data, "log_data/*.json"

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df['page'] == 'NextSong')

    # extract columns for users table    
    users_table = log_df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').dropDuplicates()
    
    # write users table to parquet files
    output_users_table = users_table.write.parquet(output_data + "output-users", "overwrite")

    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x : datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    log_df = log_df.withColumn("date_timestamp",get_datetime(col("ts")))
    
    # extract columns to create time table from the created datetime column
    time_table = log_df.select('date_timestamp')
    time_table = time_table.select('date_timestamp', 
                                   hour('date_timestamp').alias('hour'),
                                   dayofmonth('date_timestamp').alias('day'),
                                   weekofyear('date_timestamp').alias('week'), 
                                   month('date_timestamp').alias('month'),
                                   year('date_timestamp').alias('year'))
    
    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.partitionBy("year", "month").parquet(output_data + "output-times", "overwrite")

    # read in song data to use for songplays table
    song_df = os.path.join (input_data, "song_data/*/*/*/*.json" )
    song_df = spark.read.json(song_df)

    # extract columns from joined song and log datasets to create songplays table using title, duration and artist_name
    songplays_table = song_df.join(log_df, (song_df.title == log_df.song) & (song_df.duration == log_df.length) & (song_df.artist_name == log_df.artist ))
    # since songplay_id is serial generated we used monotonically_increasing_id to genrate it. 
    songplays_table = songplays_table.withColumn("songplay_id",monotonically_increasing_id())
    tsFormat = "yyyy/MM/dd HH:MM:ss z"
    # Creating the whole songplay table by selecting the multiple fields
    songplays_table = songplays_table.withColumn(
        'start_time', to_timestamp(date_format((col("ts")/1000).cast(dataType=TimestampType()),tsFormat),tsFormat)).select("songplay_id","start_time",col("userId").alias("user_id"),"level","song_id","artist_id",col("sessionId").alias("session_id"),col("artist_location").alias("location"),"userAgent",month(col("start_time")).alias("month"),year(col("start_time")).alias("year"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data,"songplays"),"overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3/buckets/output-fact-table-data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
