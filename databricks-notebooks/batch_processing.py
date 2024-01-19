# Databricks notebook source
# MAGIC %md
# MAGIC ### Read AWS keys

# COMMAND ----------

# MAGIC %run "./get_aws_keys"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount S3 bucket to Databricks

# COMMAND ----------

def is_S3_mounted(mount_name:str):
    for mount in dbutils.fs.ls("/mnt"):
        if mount_name in mount.name:
            print(f"{mount.name} : {mount.path}")
            return True
    return False

# Mount name for the bucket
MOUNT_NAME = "pinterest_data_s3_12e37"

if not is_S3_mounted(mount_name=MOUNT_NAME):
    
    # AWS S3 bucket name
    AWS_S3_BUCKET = "user-12e371d757c1-bucket"

    # Source url
    SOURCE_URL = "s3a://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)

    # Mount the drive
    dbutils.fs.mount(SOURCE_URL, "/mnt/" + MOUNT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data for the pin, geo and user topics from the mounted S3 into corresponding 3 DataFrames

# COMMAND ----------

# Sample file_location = "/mnt/pinterest_data_s3_12e37/topics/12e371d757c1.geo/partition=0/*.json" 

def read_data(path):
    """ Read all .json files from the mounted S3 bucket
    """
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

# MAGIC %md
# MAGIC ### Data Cleaning
# MAGIC

# COMMAND ----------

# MAGIC %run "./data_cleaning"

# COMMAND ----------

cleaned_df_pin = clean_df_pin(df_pin)
cleaned_df_geo = clean_df_geo(df_geo)
cleaned_df_user = clean_df_user(df_user)
