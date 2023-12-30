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

# Cell for debugging
# display(aws_keys_df)

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

# Cell for debugging
# print(ACCESS_KEY)
# print(SECRET_KEY)
# print(ENCODED_SECRET_KEY)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the S3 Bucket

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
# MAGIC ### Check if the S3 bucket was mounted successfully

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{MOUNT_NAME}/topics/"))

# COMMAND ----------

# Cell for debugging
# dbutils.fs.unmount(f"/mnt/{MOUNT_NAME}")

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

# # Display DataFrames
# display(df_pin)
# display(df_geo)
# display(df_user)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Cleaning
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean the `df_pin` DataFrame by performing the following transformations:
# MAGIC - Replace empty entries and entries with no relevant data in each column with Nones
# MAGIC - Perform the necessary transformations on the follower_count to ensure every entry is a number. Make sure the data type of this column is an int.
# MAGIC - Ensure that each column containing numeric data has a numeric data type
# MAGIC - Clean the data in the save_location column to include only the save location path
# MAGIC - Rename the index column to ind.
# MAGIC - Reorder the DataFrame columns

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

def clean_df_pin(df_pin):
    """Clean the DataFrame that contains information about Pinterest posts
    """
    # Drop duplicate rows with matching index values
    cleaned_df_pin = df_pin.dropDuplicates(['index'])
    
    # 1. Replace empty entries and entries with no relevant data in each column with Nones
    cleaned_df_pin = cleaned_df_pin.replace({'User Info Error': None}, subset=['follower_count', 'poster_name'])
    cleaned_df_pin = cleaned_df_pin.replace({'Image src error.': None}, subset=['image_src'])
    cleaned_df_pin = cleaned_df_pin.replace({'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e': None}, subset=['tag_list'])
    cleaned_df_pin = cleaned_df_pin.replace({'No description available': None, 'No description available Story format': None}, subset=['description'])
    cleaned_df_pin = cleaned_df_pin.replace({'No Title Data Available': None}, subset=['title'])
    
    # 2. Convert the 'follower_count' values listed in 'k' or 'M' to the corresponding numeric values
    cleaned_df_pin = cleaned_df_pin.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
    cleaned_df_pin = cleaned_df_pin.withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))

    # 3. Ensure that each column containing numeric data has a numeric data type

    # Change data type of follower_count column to int
    cleaned_df_pin = cleaned_df_pin.withColumn("follower_count", cleaned_df_pin["follower_count"].cast("int"))
    
    # 4. Clean the data in the save_location column to include only the save location path
    cleaned_df_pin = cleaned_df_pin.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))
    
    # 5.  Rename the index column to 'ind'
    cleaned_df_pin = cleaned_df_pin.withColumnRenamed("index", "ind")
    
    # 6. Reorder the DataFrame columns
    cleaned_df_pin = cleaned_df_pin.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category", "downloaded")
    
    return cleaned_df_pin

cleaned_df_pin = clean_df_pin(df_pin)
display(cleaned_df_pin)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean the `df_geo` DataFrame by performing the following transformations:
# MAGIC - Create a new column coordinates that contains an array based on the latitude and longitude columns
# MAGIC - Drop the latitude and longitude columns from the DataFrame
# MAGIC - Convert the timestamp column from a string to a timestamp data type
# MAGIC - Reorder the DataFrame columns
# MAGIC

# COMMAND ----------

def clean_df_geo(df_geo):
    """Clean the DataFrame that contains information about geolocation of the posts
    """
    # Drop duplicate rows with matching ind values
    cleaned_df_geo = df_geo.dropDuplicates(['ind'])
    
    # 1. Create a new column coordinates that contains an array based on the latitude and longitude columns
    cleaned_df_geo = cleaned_df_geo.withColumn("coordinates", array("latitude", "longitude"))

    # 2. Drop the latitude and longitude columns from the DataFrame
    cleaned_df_geo = cleaned_df_geo.drop("latitude", "longitude")

    # 3. Convert the timestamp column from a string to a timestamp data type
    cleaned_df_geo = cleaned_df_geo.withColumn("timestamp", to_timestamp("timestamp"))

    # 4. Reorder the DataFrame columns to have the following column order:
    cleaned_df_geo = cleaned_df_geo.select("ind", "country", "coordinates", "timestamp")
    
    return cleaned_df_geo

cleaned_df_geo = clean_df_geo(df_geo)
display(cleaned_df_geo)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean the `df_user` DataFrame by performing the following transformations:
# MAGIC - Create a new column user_name that concatenates the information found in the first_name and last_name columns
# MAGIC - Drop the first_name and last_name columns from the DataFrame
# MAGIC - Convert the date_joined column from a string to a timestamp data type
# MAGIC - Reorder the DataFrame columns

# COMMAND ----------

def clean_df_user(df_user):
    """Clean the DataFrame that contains information about users
    """
    # Drop duplicate rows with matching ind values
    cleaned_df_user = df_user.dropDuplicates(['ind'])
    
    # 1. Create a new column user_name that concatenates the information found in the first_name and last_name columns
    cleaned_df_user = cleaned_df_user.withColumn("user_name", concat("first_name", lit(" "), "last_name"))

    # 2. Drop the first_name and last_name columns from the DataFrame
    cleaned_df_user = cleaned_df_user.drop("first_name", "last_name")

    # 3. Convert the date_joined column from a string to a timestamp data type
    cleaned_df_user = cleaned_df_user.withColumn("date_joined", to_timestamp("date_joined"))

    # 4. Reorder the DataFrame columns
    cleaned_df_user = cleaned_df_user.select("ind", "user_name", "age", "date_joined")
    
    return cleaned_df_user

cleaned_df_user = clean_df_user(df_user)
display(cleaned_df_user)

# COMMAND ----------

cleaned_df_pin.printSchema()
cleaned_df_geo.printSchema()
cleaned_df_user.printSchema()

# COMMAND ----------

cleaned_df_pin.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Queries

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Find the most popular category in each country
# MAGIC Find the most popular Pinterest category people post to based on their country.
# MAGIC
# MAGIC Query should return a DataFrame that contains the following columns:
# MAGIC - country
# MAGIC - category
# MAGIC - category_count, a new column containing the desired query output

# COMMAND ----------

from pyspark.sql import Window
# Join the DataFrames
joined_df = cleaned_df_pin.join(cleaned_df_geo, cleaned_df_pin.ind == cleaned_df_geo.ind) \
                     .select("country", "category")

# Group by country and category, and count the occurrences
result_df = joined_df.groupBy("country", "category").agg(count("*").alias("category_count"))

# Rank the categories within each country based on the category count
window_spec = Window.partitionBy("country").orderBy(desc("category_count"))
result_df = result_df.withColumn("rank", rank().over(window_spec))

# Filter to get the most popular category in each country
result_df = result_df.filter("rank = 1").drop("rank")

# Show the result DataFrame
display(result_df)

# COMMAND ----------

# Tally using SQL: 
    
cleaned_df_pin.createOrReplaceTempView("cleaned_df_pin")
cleaned_df_geo.createOrReplaceTempView("cleaned_df_geo")

sql_df1 = spark.sql("""
                WITH category_totals AS (
                    SELECT country, category, COUNT(category) as category_count
                    FROM   cleaned_df_pin
                    INNER JOIN
                        cleaned_df_geo ON cleaned_df_pin.ind = cleaned_df_geo.ind
                    GROUP BY country, category), 
                category_ranks AS (
                    SELECT country, category, category_count,
                        RANK() OVER (
                            PARTITION BY country
                            ORDER BY category_count DESC
                        ) as category_rank
                    FROM category_totals)
                SELECT country, category, category_count
                FROM   category_ranks
                WHERE  category_rank = 1
                ORDER BY country;
                """)

display(sql_df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Find which was the most popular category each year
# MAGIC Find how many posts each category had between 2018 and 2022.
# MAGIC
# MAGIC Query should return a DataFrame that contains the following columns:
# MAGIC - post_year, a new column that contains only the year from the timestamp column
# MAGIC - category
# MAGIC - category_count, a new column containing the desired query output

# COMMAND ----------

# Extract the year from the timestamp column in cleaned_geo
cleaned_df_geo = cleaned_df_geo.withColumn("post_year", year("timestamp"))

# Join the dataframes on ind
joined_df = cleaned_df_pin.join(cleaned_df_geo, cleaned_df_pin.ind == cleaned_df_geo.ind)

# Filter posts between 2018 and 2022
filtered_df = joined_df.filter(year("timestamp").between(2018, 2022))

# Group by year and category, count the number of posts, and find the most popular category each year
result_df = (
    filtered_df.groupBy("post_year", "category")
    .count()
    .withColumn(
        "rn",
        row_number().over(Window.partitionBy("post_year").orderBy(desc("count"))),
    )
    .filter("rn = 1")
    .select("post_year", "category", "count")
    .withColumnRenamed("count", "category_count")
)

# Show the result
display(result_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Find the user with most followers in each country
# MAGIC
# MAGIC Step 1: For each country find the user with the most followers.
# MAGIC Query should return a DataFrame that contains the following columns:
# MAGIC - country
# MAGIC - poster_name
# MAGIC - follower_count
# MAGIC
# MAGIC Step 2: Based on the above query, find the country with the user with most followers.
# MAGIC Query should return a DataFrame that contains the following columns:
# MAGIC - country
# MAGIC - follower_count
# MAGIC
# MAGIC This DataFrame should have only one entry.

# COMMAND ----------

# Step 1: For each country find the user with the most followers

# Join the relevant DataFrames
joined_df = cleaned_df_pin.join(cleaned_df_geo, cleaned_df_pin.ind == cleaned_df_geo.ind)

most_followed_by_country_df = (
    joined_df
    .groupBy("country", "poster_name")
    .agg(max("follower_count").alias("follower_count"))
    .select("country", "poster_name", "follower_count")
)

# Step 2: Based on the above query, find the country with the user with most followers
most_followed_users_country_df = (
    most_followed_by_country_df
    .groupBy("country")
    .agg(max("follower_count").alias("follower_count"))
    .orderBy(col("follower_count").desc())
    .limit(1)
    .select("country", "follower_count")
)

display(most_followed_users_country_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Find the most popular category by different age groups
# MAGIC What is the most popular category people post to based on the following age groups: 
# MAGIC 18-24, 25-35, 36-50, +50
# MAGIC
# MAGIC Query should return a DataFrame that contains the following columns:
# MAGIC - age_group, a new column based on the original age column
# MAGIC - category
# MAGIC - category_count, a new column containing the desired query output

# COMMAND ----------

# Step 1: Join the relevant DataFrames
joined_df = cleaned_df_pin.join(cleaned_df_user, cleaned_df_pin.ind == cleaned_df_user.ind)

# Step 2: Define column for age groups
joined_df = joined_df.withColumn(
    "age_group",
    when((col("age") >= 18) & (col("age") <= 24), "18-24")
    .when((col("age") >= 25) & (col("age") <= 35), "25-35")
    .when((col("age") >= 36) & (col("age") <= 50), "36-50")
    .when(col("age") > 50, "50+")
    .otherwise("Unknown")
)

# Step 3: Group by age_group and category, then count the occurrences
category_count_df = (
    joined_df
    .groupBy("age_group", "category")
    .agg({"category": "count"})
    .withColumnRenamed("count(category)", "category_count")
)

# Step 4: Find the most popular category for each age group
window_spec = Window.partitionBy("age_group").orderBy(col("category_count").desc())
most_popular_category_df = (
    category_count_df
    .withColumn("rank", row_number().over(window_spec))
    .filter(col("rank") == 1)
    .select("age_group", "category", "category_count")
)

display(most_popular_category_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Find the median follower count for different age groups
# MAGIC What is the median follower count for users in the following age groups: 18-24, 25-35, 36-50, +50  
# MAGIC
# MAGIC Query should return a DataFrame that contains the following columns:
# MAGIC - age_group, a new column based on the original age column
# MAGIC - median_follower_count, a new column containing the desired query output

# COMMAND ----------

from pyspark.sql.functions import col, when, percentile_approx

# Step 1: Join the relevant DataFrames
joined_df = cleaned_df_pin.join(cleaned_df_user, cleaned_df_pin.ind == cleaned_df_user.ind)

# Step 2: Define column for age groups
joined_df = joined_df.withColumn(
    "age_group",
    when((col("age") >= 18) & (col("age") <= 24), "18-24")
    .when((col("age") >= 25) & (col("age") <= 35), "25-35")
    .when((col("age") >= 36) & (col("age") <= 50), "36-50")
    .when(col("age") > 50, "50+")
    .otherwise("Unknown")
)

# Step 3: Calculate the median follower count for each age group
median_follower_count_df = (
    joined_df
    .groupBy("age_group")
    .agg(percentile_approx("follower_count", 0.5).alias("median_follower_count"))
    .orderBy("age_group")
)

display(median_follower_count_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Find how many users have joined each year
# MAGIC Find how many users have joined between 2015 and 2020.
# MAGIC
# MAGIC Query should return a DataFrame that contains the following columns:
# MAGIC - post_year, a new column that contains only the year from the timestamp column
# MAGIC - number_users_joined, a new column containing the desired query output

# COMMAND ----------

# Step 1: Extract the year from the timestamp
cleaned_df_user = cleaned_df_user.withColumn("post_year", year("date_joined"))

# Step 2: Filter the data for users who joined between 2015 and 2020
users_joined_df = (
    cleaned_df_user
    .filter((col("post_year") >= 2015) & (col("post_year") <= 2020))
    .groupBy("post_year")
    .agg({"ind": "count"})
    .withColumnRenamed("count(ind)", "number_users_joined")
)

display(users_joined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Find the median follower count of users based on their joining year
# MAGIC
# MAGIC Find the median follower count of users have joined between 2015 and 2020.
# MAGIC
# MAGIC Query should return a DataFrame that contains the following columns:
# MAGIC - post_year, a new column that contains only the year from the timestamp column
# MAGIC - median_follower_count, a new column containing the desired query output

# COMMAND ----------

from pyspark.sql.functions import col, when, percentile_approx

# Step 1: Extract the year from the timestamp
cleaned_df_user = cleaned_df_user.withColumn("joining_year", year("date_joined"))

# Step 2: Join the relevant DataFrames
joined_df = cleaned_df_pin.join(cleaned_df_user, cleaned_df_pin.ind == cleaned_df_user.ind)

# Step 3: Calculate the median follower count for each post_year
median_follower_count_df = (
    joined_df
    .groupBy("joining_year")
    .agg(percentile_approx("follower_count", 0.5).alias("median_follower_count"))
)

display(median_follower_count_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Find the median follower count of users based on their joining year and age group
# MAGIC
# MAGIC Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of.
# MAGIC
# MAGIC Query should return a DataFrame that contains the following columns:
# MAGIC - age_group, a new column based on the original age column
# MAGIC - joining_year, a new column that contains only the year from the timestamp column
# MAGIC - median_follower_count, a new column containing the desired query output
# MAGIC

# COMMAND ----------

# Step 1: Join the relevant DataFrames
joined_df = cleaned_df_pin.join(cleaned_df_user, cleaned_df_pin.ind == cleaned_df_user.ind)

# Step 2: Extract the year from the timestamp
joined_df = joined_df.withColumn("joining_year", year("date_joined"))

# Step 3: Define column for age groups
joined_df = joined_df.withColumn(
    "age_group",
    when((col("age") >= 18) & (col("age") <= 24), "18-24")
    .when((col("age") >= 25) & (col("age") <= 35), "25-35")
    .when((col("age") >= 36) & (col("age") <= 50), "36-50")
    .when(col("age") > 50, "50+")
    .otherwise("Unknown")
)

# Step 4: Filter the data for users who joined between 2015 and 2020
filtered_df = joined_df.filter((col("joining_year") >= 2015) & (col("joining_year") <= 2020))

# Step 5: Calculate the median follower count for each age group and post_year
median_follower_count_df = (
    filtered_df
    .groupBy("age_group", "joining_year")
    .agg(percentile_approx("follower_count", 0.5).alias("median_follower_count"))
    .orderBy("joining_year", "age_group")
)

display(median_follower_count_df)
