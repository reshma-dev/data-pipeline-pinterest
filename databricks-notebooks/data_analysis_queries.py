# Databricks notebook source
# MAGIC %md
# MAGIC ### Run `batch_processing` notebook to read and clean data
# MAGIC Cleaned data will be available in `cleaned_df_pin`, `cleaned_df_geo` and `cleaned_df_user` dataframes, ready for running analysis queries
# MAGIC

# COMMAND ----------

# MAGIC %run "./batch_processing"

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
