# Pinterest Data Pipeline
Stream and batch data processing pipeline for ML systems hosted on AWS

- [Pinterest Data Pipeline](#pinterest-data-pipeline)
  - [Project brief](#project-brief)
  - [Architecture diagram - Pinterest Pipeline built on AWS](#architecture-diagram---pinterest-pipeline-built-on-aws)
  - [Setup](#setup)
    - [Emulation data](#emulation-data)
    - [Batch Processing](#batch-processing)
  - [Batch Processing in Databricks](#batch-processing-in-databricks)
  - [Databricks Workloads orchestration](#databricks-workloads-orchestration)
  - [Stream processing](#stream-processing)
  - [Delta Tables can be viewed in the Catalog in Databricks](#delta-tables-can-be-viewed-in-the-catalog-in-databricks)
    - [For example, the columns for the Delta table for pin](#for-example-the-columns-for-the-delta-table-for-pin)
    - [Sample data in the geo table](#sample-data-in-the-geo-table)



## Project brief
Pinterest has world-class machine learning engineering systems. They have billions of user interactions such as image uploads or image clicks which they need to process every day to inform the decisions to make. Brief of this project was to build the system in the cloud that takes in those events and runs them through two separate pipelines. One for computing real-time metrics (like profile popularity, which would be used to recommend that profile in real-time), and another for computing metrics that depend on historical data (such as the most popular category this year).


## Architecture diagram - Pinterest Pipeline built on AWS
![PinterestCloudPipelineArchitectureDiagram](/media/CloudPinterestPipelineArchitecture.png)


## Setup
### Emulation data
Get the emulation data from an RDS database, which contains three tables with data resembling data received by the Pinterest API when a POST request is made by a user uploading data to Pinterest:

- `pinterest_data` contains data about posts being updated to Pinterest
- `geolocation_data` contains data about the geolocation of each Pinterest post found in pinterest_data
- `user_data` contains data about the user that has uploaded each post found in pinterest_data

### Batch Processing
An Amazon EC2 instance in the us-east-1 region is used as an Apache Kafka client machine.
- Install `Kafka` on the EC2 instance (v`2.12-2.8.1` to match the version running on the MSK cluster setup for the project)
- Install the `IAM MSK authentication package` to connect to the MSK cluster using IAM authentication and configure the Kafka client to use AWS IAM authentication to connect to the cluster.
- Create 3 Kafka topics on the MSK cluster:
  - <UserId>.pin for the Pinterest posts data
  - <UserId>.geo for the post geolocation data
  - <UserId>.user for the post user data
- Use MSK Connect to connet the MSK cluster to an S3 bucket so that any data going through the cluster will be automatically saved and stored in a dedicated S3 bucket.
- Configure an API in API Gateway to send data to the MSK cluster to replicate the Pinterest's experimental data pipeline
- Send data to the Kafka topics using the API Invoke URL - 3 tables to 3 corresponding topics in the MSK cluster
- Data will get stored in the S3 bucket: 
  
  Topics in S3: 
  ![Data from Kafka to S3 topics](/media/topics_in_s3.png)  
  
  JSONs in the patition for topic `geo`
  ![Data files for geo](/media/jsons_for_topic_geo.png)  


## Batch Processing in Databricks
Data is read from AWS S3 bucket into Databricks and the cleaning and computations on that data are performed using Spark on Databricks

- Mount S3 bucket to Databricks
- Read in the JSONs from S3 from `topics/<UserId>.pin/partition=0/` into 3 DataFrames:
  - `df_pin` for the Pinterest post data
  - `df_geo` for the geolocation data
  - `df_user` for the user data
- Databricks notebook: [batch_processing.ipynb](/databricks-notebooks/batch_processing.ipynb)
- After cleaning the data in the DataFrames, the data is analysed. Data queries can be found in the notebook [data_analysis_queries.ipynb](/databricks-notebooks/data_analysis_queries.ipynb)


## Databricks Workloads orchestration  
Databricks workloads are orchestrated on AWS Managed Workflows for Apache Airflow (MWAA)  
![DAG running in Airflow](/media/dag_in_airflow.png)  


## Stream processing
Send streaming data to AWS Kinesis and read the data in Databricks

- Using Kinesis Data Streams create three data streams, one for each Pinterest table:
  - `streaming-<UserId>-pin`
  - `streaming-<UserId>-geo`
  - `streaming-<UserId>-user`
- Configure an API with Kinesis proxy integration to invoke the following actions:
  - List streams in Kinesis
  - Create, describe and delete streams in Kinesis
  - Add records to streams in Kinesis
- Send requests to the API which adds one record at a time to the created streams. Data from the three Pinterest tables is sent to their corresponding Kinesis stream.
- Read data from the three Kinesis streams into Databricks notebook and clean the data  [stream_processing.ipynb](/databricks-notebooks/stream_processing.ipynb)
- Cleaned data is written to Delta Tables:
  - `<UserId>_pin_table` 
  - `<UserId>_geo_table`
  - `<UserId>_user_table`


## Delta Tables can be viewed in the Catalog in Databricks

### For example, the columns for the Delta table for pin
![Delta Table columns for pin](/media/dbcatalog_pin_table_columns.png)

### Sample data in the geo table
![Sample Data for geo](/media/dbcatalog_geo_sample_data.png)
