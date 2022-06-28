import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import udf, col, year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, max, monotonically_increasing_id
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """create a spark session
    
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process song data:
    * load input data from S3 into staging table df
    * extract data for song table
    * extract data for artist table
    * save tables as .parquet files in S3 specified beforehand
    
    Key arguments:
    spark -- Spark session
    input_data -- location of S3 bucket where data resides
    output_data -- location of S3 bucket where output data should be stored
    
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    song_data_schema = StructType([
        StructField("artist_id", StringType(), False),
        StructField("artist_latitude", StringType(), True),
        StructField("artist_longitude", StringType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), False),
        StructField("song_id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("duration", DoubleType(), False),
        StructField("year", IntegerType(), False)
    ])
    df = spark.read.json(song_data, schema=song_data_schema)
    
    print("staging_songs table created")
    
    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
        output_data + "songs_table.parquet",
        mode="overwrite",
        partitionBy=["year", "artist_id"]
    )
    
    print("songs table created")
    
    # extract columns to create artists table
    artists_table = df.select(
        "artist_id",
        col("artist_name").alias("name"),
        col("artist_location").alias("location"),
        col("artist_latitude").alias("latitude"),
        col("artist_longitude").alias("longitude")
        ).distinct()

    print("artists table created")
    
    # write artists table to parquet files
    artists_table.write.parquet(
        output_data + "artists_table.parquet",
        mode="overwrite"
    )


def process_log_data(spark, input_data, output_data):
    """Process log data:
    * load input data from S3 into staging table df
    * extract data for users table
    * extract data for time table
    * join songs and artist table with staging_log table to create songplays table 
    
    Key arguments:
    spark -- Spark session
    input_data -- location of S3 bucket where data resides
    output_data -- location of S3 bucket where output data should be stored
    
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
    
    # read log data file
    log_data_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), False),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), False),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), False),
        StructField("location", StringType(), True),
        StructField("method", StringType(), False),
        StructField("page", StringType(), False),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", IntegerType(), False),
        StructField("song", StringType(), True),
        StructField("status", IntegerType(), False),
        StructField("ts", DoubleType(), False),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True)
    ])
    df = spark.read.json(log_data, schema=log_data_schema)
    
    print("staging_logs table created")
    
    # filter by actions for song plays
    df = df.filter(col("page")== "NextSong")

    # extract columns for users table    
    users_table = (
         df
        .withColumn("max_ts_user", max("ts").over(Window.partitionBy("userID")))
        .filter(
            (col("ts") == col("max_ts_user")) &
            (col("userID") != "") &
            (col("userID").isNotNull())
        )
        .select(
            col("userID").alias("user_id"),
            col("firstName").alias("first_name"),
            col("lastName").alias("last_name"),
            "gender",
            "level"
        )
    )
    # write users table to parquet files
    users_table.write.parquet(
        output_data + "users_table.parquet", mode="overwrite"
    )
    
    print("users table created")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(
        lambda x: datetime.fromtimestamp(x / 1000).replace(microsecond=0),
        TimestampType()
    )
    df = df.withColumn("start_time", get_timestamp("ts"))
        
    # extract columns to create time table
    time_table = (
        df
        .withColumn("hour", hour("start_time"))
        .withColumn("day", dayofmonth("start_time"))
        .withColumn("week", weekofyear("start_time"))
        .withColumn("month", month("start_time"))
        .withColumn("year", year("start_time"))
        .withColumn("weekday", dayofweek("start_time"))
        .select("start_time", "hour", "day", "week", "month", "year", "weekday")
        .distinct()
    )
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(
        output_data + "time_table.parquet", mode="overwrite"
    )
    
    print("time table created")
    
    # read in song and artists data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs_table.parquet")
    artists_table = spark.read.parquet(output_data + "artists_table.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songs = (
        song_df
        .join(artists_table, "artist_id", "full")
        .select("song_id", "title", "artist_id", "name", "duration")
    )
    songplays_table = df.join(
        songs,
        [
            df.song == songs.title,
            df.artist == songs.name,
            df.length == songs.duration
        ],
        "left"
    )
    songplays_table = (
        songplays_table
        .join(time_table, "start_time", "left")
        .select(
            "start_time",
            col("userId").alias("user_id"),
            "level",
            "song_id",
            "artist_id",
            col("sessionId").alias("session_id"),
            "location",
            col("userAgent").alias("user_agent"),
            "year",
            "month"
        )
        .withColumn("songplay_id", monotonically_increasing_id())
    )
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(
        output_data + "songplays_table.parquet",
        mode="overwrite",
        partitionBy=["year", "month"]
    )

    print("songplays table created")
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://silviomartin-udac-9087/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
