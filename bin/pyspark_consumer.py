from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import Normalizer, StandardScaler
import random
from app import app
import time

""" Heard Health Monitor"""
kafka_topic_name = app.config['TOPIC_NAME']
kafka_bootstrap_servers = app.config['KAFKA_SERVER']

spark = SparkSession \
    .builder \
    .appName("Structured Streaming ") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Construct a streaming DataFrame that reads from topic
cow_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "latest") \
    .load()

cow_df1 = cow_df.selectExpr("CAST(value AS STRING)", "timestamp")

cow_schema_string = "order_id INT," \
                       "day STRING," \
                       "animal STRING," \
                       "lie DOUBLE," \
                       "stand DOUBLE," \
                       "walk DOUBLE," \
                       "ruminate DOUBLE," \
                       "grazing DOUBLE," \
                       "nothing DOUBLE," \
                       "inactive DOUBLE," \
                       "active DOUBLE," \
                       "highActive DOUBLE"

cow_df2 = cow_df1.select(from_csv(col("value"), cow_schema_string).alias("cow"), "timestamp")

cow_df3 = cow_df2.select("cow.*", "timestamp")

cow_df3.createOrReplaceTempView("cow_find")
song_find_text = spark.sql("SELECT * FROM cow_find")
cow_agg_write_stream = song_find_text \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("append") \
    .option("truncate", "false") \
    .format("memory") \
    .queryName("testedTable") \
    .start()

cow_agg_write_stream.awaitTermination(1)