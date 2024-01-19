# Databricks notebook source
# MAGIC %md
# MAGIC ### Get the AWS access key and secret key

# COMMAND ----------

# MAGIC %run "./get_aws_keys"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Explicit schemas for the three streams

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType

schema_pin = StructType([StructField("index", LongType(), True),
                         StructField("unique_id", StringType(), True),
                         StructField("title", StringType(), True),
                         StructField("description", StringType(), True),
                         StructField("follower_count", StringType(), True),
                         StructField("poster_name", StringType(), True),
                         StructField("tag_list", StringType(), True),
                         StructField("is_image_or_video", StringType(), True),
                         StructField("image_src", StringType(), True),
                         StructField("save_location", StringType(), True),
                         StructField("category", StringType(), True),
                         StructField("downloaded", StringType(), True) ])

schema_geo = StructType([StructField("ind", LongType(), True),
                         StructField("country", StringType(), True),
                         StructField("latitude", StringType(), True),
                         StructField("longitude", StringType(), True),
                         StructField("timestamp", StringType(), True) ])

schema_user = StructType([StructField("ind", LongType(), True),
                          StructField("first_name", StringType(), True),
                          StructField("last_name", StringType(), True),
                          StructField("age", StringType(), True),
                          StructField("date_joined", StringType(), True)])

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function to read data from Kinesis streams into Databricks

# COMMAND ----------

from pyspark.sql.functions import *

def read_stream(schema:StructType, stream_name:str):
    return spark.readStream \
                .format('kinesis') \
                .option('streamName', stream_name) \
                .option('initialPosition','earliest') \
                .option('region','us-east-1') \
                .option('awsAccessKey', ACCESS_KEY) \
                .option('awsSecretKey', SECRET_KEY) \
                .load() \
                .selectExpr("CAST(data as STRING) jsonData") \
                .select(from_json("jsonData", schema=schema).alias("data")) \
                .select("data.*")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data for the pin, geo and user data streams into corresponding 3 DataFrames

# COMMAND ----------

df_pin = read_stream(schema_pin, 'streaming-12e371d757c1-pin')
df_geo = read_stream(schema_geo, 'streaming-12e371d757c1-geo')
df_user = read_stream(schema_user, 'streaming-12e371d757c1-user')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function to write data to Delta table

# COMMAND ----------

def write_data(df, table_name):
    df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .queryName("query_" + table_name) \
    .option("checkpointLocation", "/tmp/kinesis/_checkpoints/12e37/" + table_name + "/") \
    .table(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC NOTE: The .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") allows us to recover the previous state of a query in case of failure. Before running the writeStream function again, delete the checkpoint folder using the following command:

# COMMAND ----------

# dbutils.fs.rm("/tmp/kinesis/_checkpoints/12e37/", True)
# dbutils.fs.rm("/tmp/kinesis/_checkpoints/" + "12e37_" + "12e371d757c1_geo_table", True)
# dbutils.fs.rm("/tmp/kinesis/_checkpoints/" + "12e37_" + "12e371d757c1_user_table", True)
# dbutils.fs.rm("/tmp/kinesis/_checkpoints/" + "12e37_" + "12e371d757c1_pin_table", True)

# COMMAND ----------

# MAGIC %run "./data_cleaning"

# COMMAND ----------

cleaned_df_pin = clean_df_pin(df_pin)
cleaned_df_geo = clean_df_geo(df_geo)
cleaned_df_user = clean_df_user(df_user)

# COMMAND ----------

write_data(cleaned_df_pin, '12e371d757c1_pin_table')

# COMMAND ----------

write_data(cleaned_df_geo, '12e371d757c1_geo_table')

# COMMAND ----------

write_data(cleaned_df_user, '12e371d757c1_user_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE 12e371d757c1_geo_table;
# MAGIC -- DROP TABLE 12e371d757c1_user_table;
# MAGIC -- DROP TABLE 12e371d757c1_pin_table;

# COMMAND ----------


