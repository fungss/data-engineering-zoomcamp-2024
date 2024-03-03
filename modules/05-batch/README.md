# Module 5 - Batch Processing
- When to use Spark
- Spark RDDs
- Connecting Spark with GCS
- Setting up a local spark cluster / Dataproc cluster

## Notes
1. Actions vs Transformations
 - execute right away (e.g. show()) vs lazy evaluation (e.g. select())

## Preparation

## Assignment
Questions can be found [here](https://github.com/fungss/data-engineering-zoomcamp-2024/blob/main/modules/05-batch/homework.md).

## Question 1 - Spark setup
Just steps to install PySpark. No answer required.

## Question 2 - Average size of created parquet files
```
ls -lh ./bucket/pq/fhv/2019/10
```
Answer: 6MB

## Question 3 - Count taxi trips on 15th October
```
df_fhv \
    .where(lit(to_date(df_fhv.pickup_datetime)) == "2019-10-15") \
    .count()
```
Answer: 62610

## Question 4 - Length of the longest trip
```
df_fhv \
    .withColumn('diff_in_hours', round((unix_timestamp("dropoff_datetime") - unix_timestamp('pickup_datetime'))/3600, 2)) \
    .orderBy('diff_in_hours', ascending=False) \
    .select(['dispatching_base_num', 'pickup_datetime', 'dropoff_datetime', 'diff_in_hours']) \
    .show(5)
```
Answer: 631,152.50 Hours

## Question 5 - Port of locally running Spark
Answer: 4040

## Question 6 - Least frequent pickup location zone
```
df_fhv.select(['tripid', 'pulocationid']) \
    .join(df_zone_data.select(['LocationID', 'Zone']), df_fhv.pulocationid == df_zone_data.LocationID, "left") \
    .groupBy('Zone') \
    .count() \
    .orderBy('count', ascending=True) \
    .show()

```
Answer: Jamaica Bay

## Reference
1. 