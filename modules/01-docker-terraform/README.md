# Module 1 - Docker and Postgres
- Parametrize and dockerize ingestion script
- Docker networking and port mapping
- Cloud provision with Terraform

Questions of the assignment can be found [here](https://github.com/fungss/data-engineering-zoomcamp-2024/blob/main/modules/01-docker-terraform/homework.md).

## Question 1. Knowing docker tags
```
docker run --help | grep "Automatically remove the container when it exits"
```
Answer: --rm

## Question 2. Understanding docker first run
```
docker run -it --entrypoint=bash python:3.9
pip list | grep "wheel"
```
Answer: 0.42.0

## Preparation for the remaining questions
The commands below will extract and load the following data to pg-database:
1. green_tripdata_2019-09.csv, and
2. taxi+_zone_lookup.csv
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

## Question 3. Count records
```
select count(*)
from green_taxi_trips
where lpep_pickup_datetime::date = '2019-09-18'::date
and lpep_dropoff_datetime::date = '2019-09-18'::date
```
Answer: 15612

## Question 4. Largest trip for each day
```
select lpep_pickup_datetime::date, max(trip_distance::numeric)
from green_taxi_trips
where lpep_pickup_datetime::date in ('2019-09-18'::date, '2019-09-16'::date, '2019-09-26'::date, '2019-09-21'::date)
group by lpep_pickup_datetime::date
order by max(trip_distance::numeric) DESC
```
Answer: 2019-09-26

## Question 5. The number of passengers
```
with total_amount_by_borough AS (
	select zones."Borough", SUM(trips.total_amount::numeric) as borough_total
	from green_taxi_trips trips
	left join taxi_zones zones
	on trips."PULocationID" = zones."LocationID"
	where zones."Borough" != 'Unknown'
	and trips.lpep_pickup_datetime::date = '2019-09-18'::date
	group by zones."Borough"
)
select *
from total_amount_by_borough
where borough_total > 50000
order by borough_total desc
```
Answer: "Brooklyn" "Manhattan" "Queens"

## Question 6. Largest tip
```
with max_tip_amount_by_dolocation as (
	select zones."Zone", trips."DOLocationID", max(trips."tip_amount"::numeric) as max_tip_amount_by_dolocation
	-- select *
	from green_taxi_trips trips
	left join taxi_zones zones
	on trips."PULocationID" = zones."LocationID"
	where extract(year from trips.lpep_pickup_datetime::date)::text = '2019'
	and extract(month from trips.lpep_pickup_datetime::date)::text = '9'
	and zones."Zone" = 'Astoria'
	group by zones."Zone", trips."DOLocationID"
)
select zones."Zone", max_tip_amount_by_dolocation
from max_tip_amount_by_dolocation
left join taxi_zones zones
on max_tip_amount_by_dolocation."DOLocationID" = zones."LocationID"
order by max_tip_amount_by_dolocation desc
```
Answer: JFK Airport
