# Import the necessary packages
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, monotonically_increasing_id, row_number
from pyspark.sql.functions import year, month, dayofweek, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window

# Log in to AWS 
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


# This function creates a SparkSession with the configuration that enables access to Amazon S3 storage (by adding the "hadoop-aws" package to the Spark configuration). If an existing SparkSession exists, it returns that, otherwise, it creates a new one.
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

# This function reads in song data from the specified input path and extracts the relevant columns to create two tables - "songs" and "artists". It writes these tables to Parquet files, with the "songs" table partitioned by year and artist_id.
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs", mode="overwrite")

    # extract columns to create artists table
    artists_table = df.selectExpr(["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists", mode="overwrite")

# This function reads in log data from the specified input path and extracts the relevant columns to create three tables - "users", "time" and "songplays". It writes the "users" table to a Parquet file, and the "time" and "songplays" tables are partitioned by year and month before being written to separate Parquet files.    
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*.json"

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df.where(log_df.page == 'NextSong')

    # extract columns for users table    
    user_table = log_df.selectExpr(["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"])
    
    # write users table to parquet files
    user_table.write.parquet(output_data + "parquet_log/user", mode="overwrite")

    # create timestamp column from original timestamp column
    # define the UDF to convert the timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts / 1000), TimestampType())

    # create a new column with the converted timestamp
    log_df = log_df.withColumn('start_time', to_timestamp(log_df.ts))
      
    # extract columns to create time table
    time_table = log_df.select('start_time')
    time_table = time_table.withColumn('hour', hour('start_time'))
    time_table = time_table.withColumn('day', dayofmonth('start_time'))
    time_table = time_table.withColumn('week', weekofyear('start_time'))
    time_table = time_table.withColumn('month', month('start_time'))
    time_table = time_table.withColumn('year', year('start_time'))
    time_table = time_table.withColumn('weekday', dayofweek('start_time'))

    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "parquet_log/time_table", mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    # join song_df and log_df
    song_log_joined_table = log_df.join(song_df, (log_df.song == song_df.title) & (log_df.artist == song_df.artist_name) & (log_df.length == song_df.duration), how='inner')
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_log_joined_table.distinct() \
                        .select("userId", "start_time", "song_id", "artist_id", "level", "sessionId", "location", "userAgent") \
                        .withColumn("songplay_id", row_number().over( Window.partitionBy('start_time').orderBy("start_time"))) \
                        .withColumnRenamed("userId","user_id")        \
                        .withColumnRenamed("start_time","start_time")  \
                        .withColumnRenamed("sessionId","session_id")  \
                        .withColumnRenamed("userAgent", "user_agent") \
                        .withColumn('year', year('start_time')) \
                        .withColumn('month', month('start_time'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + "parquet_log/songplays", mode="overwrite")


# This function executes the programm    
def main():
    spark = create_spark_session()
    input_data = 's3://udacity-dend/'
    output_data = 'https://sparkproject23.s3.us-west-2.amazonaws.com/Output_Data/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
