# Module 2 - Workflow Orchestration
- ETL with mage
- Using PyArrow to partition result dataframe and upload to gcs (Google Cloud Storage) bucket in parquet format

## Setup
1. Provision gcs and related resources with the terraform files
2. ```docker compose --env-file=./dev.env -d``` to spin up mage server locally

## Assignment
Questions of the assignment can be found [here](https://github.com/fungss/data-engineering-zoomcamp-2024/blob/main/modules/02-workflow-orchestration/homework.md).

## Question 1. Data Loading
Answer: 266,855 rows x 20 columns

## Question 2. Data Transformation
Answer: 139,370 rows

## Question 3. Data Transformation
Answer: ```data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date```

## Question 4. Data Transformation
Answer: 1 or 2

## Question 5. Data Transformation
Answer: 4

## Question 6. Data Exporting
Answer: 96 (Supposed to be 95. Was adviced to select the closet answer.)
