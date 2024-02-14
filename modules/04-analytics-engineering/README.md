# Module 4 - Analytics Engineering
- 
- 
- 

## Notes
1. 

## Assignment
Questions can be found [here](https://github.com/fungss/data-engineering-zoomcamp-2024/blob/main/modules/04-analytics-engineering/homework.md).

## Question 1 - dbt basics
Answer: 840,402 (Shown as result in Mage's data loader block)

## Question 2 - Distribution between service type
```
-- External table
SELECT COUNT(DISTINCT PULocationID) as cnt_distinct_pu_location_id FROM `dtc-de-course-410021.nyc_taxi_data.external_green_tripdata_2022`

-- Non partitioned table
SELECT COUNT(DISTINCT PULocationID) as cnt_distinct_pu_location_id FROM dtc-de-course-410021.nyc_taxi_data.green_tripdata_2022_non_partitoned_non_clustered
```
Answer: 0 MB for the External Table and 6.41MB for the Materialized Table (See in JOB INFORMATION tab)

## Question 3 - CI job
```
SELECT COUNT(*) as cnt_fare_amount_0 
FROM dtc-de-course-410021.nyc_taxi_data.green_tripdata_2022_non_partitoned_non_clustered
WHERE fare_amount = 0
```
Answer: 1,622

## Question 4 - Record count in model fact_fhv
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

## Question 5 - Service with the most rides
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

## Reference
1. 