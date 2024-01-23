# data-pipeline-pinterest
Stream and batch data processing pipeline for ML systems hosted on AWS

## Project brief
Pinterest has world-class machine learning engineering systems. They have billions of user interactions such as image uploads or image clicks which they need to process every day to inform the decisions to make. Aim of this project was to build the system in the cloud that takes in those events and runs them through two separate pipelines. One for computing real-time metrics (like profile popularity, which would be used to recommend that profile in real-time), and another for computing metrics that depend on historical data (such as the most popular category this year).

## Pinterest Pipeline built on AWS
![PinterestCloudPipelineArchitectureDiagram](/media/CloudPinterestPipelineArchitecture.png)

## Delta Tables can be viewed in the Catalog

### For example, the columns for the Delta table for pin
![Delta Table columns for pin](/media/dbcatalog_pin_table_columns.png)

### Sample data in the geo table
![Sample Data for geo](/media/dbcatalog_geo_sample_data.png)
