# Sparkify-Spark-Project
### Introduction
This Python script is designed to process JSON log and song data with the PySpark SQL library. The script reads in log and song data from the input folder and outputs tables in the parquet format to the output folder, which are partitioned by year and month.

Requirements
Python 3.x
PySpark
configparser
datetime
os

### Usage
To use the script, you must first modify the dl.cfg file with your own AWS access key ID and secret access key. Then you can run the script with the following command:

#### Copy code
python etl.py
The script will read in log and song data from the data/ folder and output the processed tables to the output_data/ folder.

### Function: create_spark_session()
This function creates a SparkSession with the configuration that enables access to Amazon S3 storage (by adding the "hadoop-aws" package to the Spark configuration). If an existing SparkSession exists, it returns that, otherwise, it creates a new one.

### Function: process_song_data(spark, input_data, output_data)
This function reads in song data from the specified input path and extracts the relevant columns to create two tables - "songs" and "artists". It writes these tables to Parquet files, with the "songs" table partitioned by year and artist_id.

### Function: process_log_data(spark, input_data, output_data)
This function reads in log data from the specified input path and extracts the relevant columns to create three tables - "users", "time" and "songplays". It writes the "users" table to a Parquet file, and the "time" and "songplays" tables are partitioned by year and month before being written to separate Parquet files.

The "time" table is created by extracting the timestamp from the "ts" column and then breaking it down into individual columns representing hour, day, week, month, year, and weekday.

The "songplays" table is created by joining the log data with the song data on the fields "song", "artist", and "length". It then selects the relevant columns from the joined table and adds a "songplay_id" column, which is used to identify unique song plays.
