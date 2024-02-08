# Module 3 - Data Warehouse (BigQuery)
- Paritioning and Clustering
- Architecture of BigQuery
- BigQuery Best Practices

## Notes
1. Partitioning (bq limit: 4000)
 - Break huge dataset into smaller chunks, facilitating parallel processing to reduce query processing time. It can also reduce query cost by 1) reducing total amount data to be processed (1.6GB -> 106MB) and 2) size of each chunk to be processed.
 - The goal for partitioning is to faciliate parallel processing. To maximize benefit from partitioning a huge dataset the partition key should have the right amount of values available such that ideally the distribution of the partitioned datasets is uniform and the key would be used for query that operates evenly on each of the partitions.
2. Clustering (bq limit: 4)
 - Physically sort and store data according to column values. Enabling faster search and sorting. Suitable for query with filtering, aggregation and order by statements.
3. Pricing of BigQuery
 - on-demand pricing or capacity-based pricing

## Assignment
Questions can be found [here](https://github.com/fungss/data-engineering-zoomcamp-2024/blob/main/modules/03-data-warehouse/homework.md).

## Question 1 - Record count of Green Taxi Trip Records Data for 2022
Answer: 840,402 (Shown as result in Mage's data loader block)

## Preparation
After extracting and loading the required New York City Taxi Data to GCS, 
1. Create a BigQuery dataset named ```nyc_taxi_data```
2. Create 1) external table, and 2) non-partitioned table with the code below
```
-- Create external table from gcs path
-- table_id = "your-project.your_dataset.your_table_name"
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-410021.nyc_taxi_data.external_green_tripdata_2022`
OPTIONS (
  format = 'Parquet',
  uris = ['gs://module-3-bucket-989f1c79229e21c2/nyc_taxi_data/*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE dtc-de-course-410021.nyc_taxi_data.green_tripdata_2022_non_partitoned_non_clustered AS
SELECT * FROM `dtc-de-course-410021.nyc_taxi_data.external_green_tripdata_2022`;
```

## Question 2 - Count distinct number of PULocationID
```
-- External table
SELECT COUNT(DISTINCT PULocationID) as cnt_distinct_pu_location_id FROM `dtc-de-course-410021.nyc_taxi_data.external_green_tripdata_2022`

-- Non partitioned table
SELECT COUNT(DISTINCT PULocationID) as cnt_distinct_pu_location_id FROM dtc-de-course-410021.nyc_taxi_data.green_tripdata_2022_non_partitoned_non_clustered
```
Answer: 0 MB for the External Table and 6.41MB for the Materialized Table (See in JOB INFORMATION tab)

## Question 3 - Count number of records with 0 fare_amount
```
SELECT COUNT(*) as cnt_fare_amount_0 
FROM dtc-de-course-410021.nyc_taxi_data.green_tripdata_2022_non_partitoned_non_clustered
WHERE fare_amount = 0
```
Answer: 1,622

## Question 4 - Best strategy to optimize table in Big Query if query will always order the results by PUlocationID and filter based on lpep_pickup_datetime
```
-- Create a partitioned and clustered table from external table
CREATE OR REPLACE TABLE dtc-de-course-410021.nyc_taxi_data.green_tripdata_2022_partitoned_clustered
PARTITION BY lpep_pickup_date
CLUSTER BY PUlocationID AS
SELECT * FROM `dtc-de-course-410021.nyc_taxi_data.external_green_tripdata_2022`;

-- To check partition
SELECT table_name, partition_id, total_rows
FROM `nyc_taxi_data.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'green_tripdata_2022_partitoned_clustered'
ORDER BY partition_id
```
Answer: Partition by lpep_pickup_datetime Cluster on PUlocationID

## Question 5 - Distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
```
-- Non-partitioned (12.82MB)
SELECT DISTINCT PULocationID as cnt_distinct_pu_location_id FROM dtc-de-course-410021.nyc_taxi_data.green_tripdata_2022_non_partitoned_non_clustered
WHERE lpep_pickup_date BETWEEN '2022-06-01' AND '2022-06-30'

-- Partitioned (1.12MB)
SELECT distinct PUlocationID
FROM dtc-de-course-410021.nyc_taxi_data.green_tripdata_2022_partitoned_clustered
WHERE lpep_pickup_date BETWEEN '2022-06-01' AND '2022-06-30'
```
Answer: 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table

## Question 6 - Data stored in the External Table
Answer: GCP Bucket

## Question 7 - Always apply clustering
Answer: False </br>
The reason is that partitioning and clustering won't have significant improvement for table with size < 1GB (The entire table is smaller enough to fit in the memory of the compute engine and therefore [Block pruning](https://cloud.google.com/bigquery/docs/clustered-tables#block-pruning) would not kick in.)

## Question 8 - Estimated btyes for ```SELECT count(*)```
```
SELECT COUNT(*)
FROM dtc-de-course-410021.nyc_taxi_data.green_tripdata_2022_partitoned_clustered
```
Answer: 0 </br>
Not sure exactly, but found this [answer](https://stackoverflow.com/questions/53712073/bigquery-this-query-will-process-0-b-when-run-for-partitiondate) saying that the btyes is 0 because BigQuery would get this result from metadata tables without requiring to scan over the data.

## Reference
1. [A Deep Dive Into Google BigQuery Architecture: How It Works](https://panoply.io/data-warehouse-guide/bigquery-architecture/)

2. [Leveraging Google Cloud Storage for Cost Saving with BigQuery Tables](https://medium.com/@mich.talebzadeh/leveraging-google-cloud-storage-for-cost-saving-with-bigquery-tables-fe4cbbfe5d51)

3. [Introduction to clustered tables](https://cloud.google.com/bigquery/docs/clustered-tables#:~:text=Clustered%20tables%20can%20improve%20query,values%20in%20the%20clustered%20columns.)

4. [Optimizing BigQuery: Cluster your tables](https://hoffa.medium.com/bigquery-optimized-cluster-your-tables-65e2f684594b)

5. [Create and use clustered tables](https://cloud.google.com/bigquery/docs/creating-clustered-tables)

6. [How to reduce the cost of BigQuery data processing](https://www.measurelab.co.uk/blog/reduce-bigquery-cost-data-processing/)

7. [BigQuery does not support milliseconds or microseconds loading JSON](https://cloud.google.com/knowledge/kb/bigquery-does-not-support-milliseconds-microseconds-during-json-loading-from-google-cloud-storage-000004365#:~:text=for%20DATETIME%20%2F%20TIMESTAMP.-,Cause,as%20YYYY%2DMM%2DDD.)

8. [Using BigQuery Execution Plans to Improve Query Performance](https://medium.com/slalom-build/using-bigquery-execution-plans-to-improve-query-performance-af141b0cc33d)