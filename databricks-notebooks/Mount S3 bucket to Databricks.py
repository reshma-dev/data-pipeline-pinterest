# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount S3 bucket to Databricks
# MAGIC ### Verify that the credentials have been uploaded to Databricks
# MAGIC File called authentication_credentials.csv

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the csv file containing AWS keys into Databricks

# COMMAND ----------

# Specify file type to be CSV
file_type = "csv"
# Indicate file has first row as the header
first_row_is_header = "true"
# Indicate file has comma as the delimeter
delimeter = ","

# Read the CSV file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
    .option("header", first_row_is_header)\
    .option("sep", delimeter)\
    .load("/FileStore/tables/authentication_credentials.csv")

# COMMAND ----------

# Cell for debugging - DO NOT COMMIT!
display(aws_keys_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract the `Access key ID` and `Secret access key`
# MAGIC Use the spark dataframe `aws_keys_df` created above to extract the keys and encode the Secret access key using `urllib.parse.quote` for security.  
# MAGIC `safe=""` states that every character will be encoded.
# MAGIC

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']

# Encode the secret key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# Cell for debugging - DO NOT COMMIT!
# print(ACCESS_KEY)
print(SECRET_KEY)
print(ENCODED_SECRET_KEY)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the S3 Bucket

# COMMAND ----------

# AWS S3 bucket name
AWS_S3_BUCKET = "user-12e371d757c1-bucket"

# Mount name for the bucket
MOUNT_NAME = "/mnt/pinterest_data_s3_12e37"

# Source url
# Should be: s3://user-12e371d757c1-bucket/topics/12e371d757c1.geo/partition=0/
SOURCE_URL = "s3a://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)

# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

# Cell for debugging
# print(SOURCE_URL)

data = []
for mount in dbutils.fs.ls("/mnt"):
    if "12e37" in mount.name:
        print(f"{mount.name} : {mount.path}")
    data.append(mount.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check if the S3 bucket was mounted successfully

# COMMAND ----------

# display(dbutils.fs.ls("/mnt/"))
display(dbutils.fs.ls("/mnt/pinterest_data_s3_12e37/topics/"))

# COMMAND ----------

# dbutils.fs.unmount("/mnt/pinterest_data_s3_12e37")

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/pinterest_data_s3_12e37/topics/12e371d757c1.geo/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df)

# COMMAND ----------

type(df)

# COMMAND ----------

# Build file_location = "/mnt/pinterest_data_s3_12e37/topics/12e371d757c1.geo/partition=0/*.json" 

def read_data(path):
    file_type = "json"
    infer_schema = "true"
    
    # Read in JSONs from mounted S3 bucket
    df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .load(path)
    
    return df

path_to_topics = "/mnt/pinterest_data_s3_12e37/topics/"
aws_iam_username = "12e371d757c1"

# Create DataFrames for the three topics:

# df_pin for the Pinterest post data
df_pin = read_data(path_to_topics + aws_iam_username + "." + "pin" + "/partition=0/*.json")

# df_geo for the geolocation data
df_geo = read_data(path_to_topics + aws_iam_username + "." + "geo" + "/partition=0/*.json")

# df_user for the user data
df_user = read_data(path_to_topics + aws_iam_username + "." + "user" + "/partition=0/*.json")



# COMMAND ----------

# def read_topics_data(path:str, aws_iam_username:str, topics:list):
#     file_type = "json"
        
#     for i in range(len(topics)):
#         url = path + aws_iam_username + "." + topics[i] + "/partition=0/*.json"
#         exec(f'df_{topics[i]} = spark.read.format("json").option("inferSchema", True).load({url})')

# COMMAND ----------

# path_to_topics = "/mnt/pinterest_data_s3_12e37/topics/"
# aws_iam_username = "12e371d757c1"
# topics = ['pin', 'geo', 'user']
# read_topics_data(path_to_topics, aws_iam_username, topics)

# COMMAND ----------

display(df_pin)

# COMMAND ----------

display(df_geo)

# COMMAND ----------

display(df_user)

# COMMAND ----------


