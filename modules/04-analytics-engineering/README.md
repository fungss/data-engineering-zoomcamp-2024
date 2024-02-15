# Module 4 - Analytics Engineering
- 
- 
- 

## TODO
1. re-work everything in cloud
2. add ER-diagram

## Notes
1. dbt is a transformation tool that facilitates software engineering best practices like modularity, portability, CI/CD, and documentation.
2. Views
 - abstract complex joins from user
 - limit degree of exposure - avoid giving table level permissions and faciliate hiding of PII info
 - allow changing logic and behavior without changing the output structure
 - In dbt, materialized view automatically updates, while table does not.

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
1. [SQL Tables and Views: What's the Difference?](https://youtu.be/eumDqVqaCT4?si=lN2-CONmXa_kEDHc)

2. [When to use a View instead of a Table?](https://stackoverflow.com/questions/4378068/when-to-use-a-view-instead-of-a-table)

3. [Chapt-07: Using Docker Volumes in a Dockerfile to Manage Data Persistence](https://medium.com/@maheshwar.ramkrushna/docker-volume-7f9d0069f068)

4. [jeremyyeo/dbt-docker-m1](https://github.com/jeremyyeo/dbt-docker-m1/blob/master/Dockerfile)

5. [Python models](https://docs.getdbt.com/docs/build/python-models)

6. [Anything one should know before going for self-hosted dbt?](https://www.reddit.com/r/dataengineering/comments/14w832y/anything_one_should_know_before_going_for/)

7. [Materialized View vs Table Using dbt](https://stackoverflow.com/questions/64489772/materialized-view-vs-table-using-dbt#:~:text=If%20you%20have%20a%20DBT,the%20table%20by%20scheduling%20DBT.)

8. [Materializations](https://docs.getdbt.com/docs/build/materializations)