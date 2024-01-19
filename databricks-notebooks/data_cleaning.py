# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Cleaning functions for pin, user and geo DataFrames
# MAGIC

# COMMAND ----------

# Clean the `df_pin` DataFrame

from pyspark.sql.functions import *

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

# COMMAND ----------

# Clean the `df_geo` DataFrame

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

# COMMAND ----------

# Clean the `df_user` DataFrame

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
