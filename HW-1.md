# Module 1 Homework - Ronald Fung

## Question 1
```
docker run --help | grep "Automatically remove the container when it exits"
```
Answer: --rm

## Question 2
```
docker run -it --entrypoint=bash python:3.9
pip list | grep "wheel"
```
Answer: 0.42.0

## Data preparation for the remaining questions

```
docker run -it \
    --network=pg-network \
    taxi_ingest:v1 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --database_name=ny_taxi \
    --table_name=green_taxi_trips \
    --target_url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"
```

```
docker run -it \
    --network=pg-network \
    taxi_ingest:v1 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --database_name=ny_taxi \
    --table_name=taxi_zones \
    --target_url="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
```

## Question 3

