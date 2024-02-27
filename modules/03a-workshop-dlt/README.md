# Workshop - Data Load Tool (dlt)
- Use of Python Generators for incremental loading


## Notes
1. Generators handle element one at a time. It avoids loading the whole dataset in-memory, thus more memory efficient.
2. Python list stores pointers. Numpy array stores values in block - solved problem of memory fragmentation.
3. Map() is memory-efficient than applying the function with normal for-loop because it will not create copies of the original data and it will perform the transformation lazily (when the result is called)

## Assignment
Questions can be found [here](https://github.com/fungss/data-engineering-zoomcamp-2024/blob/main/modules/03a-workshop-dlt/dlt.md).

## Preparation for Question 1 and 2
```
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1
```

## Question 1 - Sum of the outputs of the generator for limit = 5
```
limit = 5
generator = square_root_generator(limit)

result = 0

for sqrt_value in generator:
    print(sqrt_value)
    result+=sqrt_value

print(f"The sum is: {result}")
```
Answer: C: 8.382332347441762

## Question 2 - 13th number yielded by the generator
```
limit = 13
generator = square_root_generator(limit)

for idx, sqrt_value in enumerate(generator, start=1):
  if idx == 13:
    print(f"The {idx} number is: {sqrt_value}")
```
Answer: B: 3.605551275463989

## Preparation for Question 3, 4, and 5
```
import dlt

def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}
```

## Question 3 - Sum of ages of all people (Append)
```
generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')

info = generators_pipeline.run(people_1(),table_name="people",write_disposition="replace")

print(info)

info = generators_pipeline.run(people_2(),table_name="people",write_disposition="append")

print(info)

conn.sql("SELECT sum(age) FROM people")
```
Answer: A: 353

## Question 4 - Sum of ages of all people (Merge by id)
```
generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')

info = generators_pipeline.run(people_1(),table_name="people",write_disposition="replace")

print(info)

info = generators_pipeline.run(people_2(),table_name="people",write_disposition="merge",primary_key="id")

print(info)

conn.sql("SELECT sum(age) FROM people")
```
Answer: B: 266

## Reference
1. [JSON Lines format: Why jsonl is better than a regular JSON for web scraping](https://hackernoon.com/json-lines-format-76353b4e588d)

2. [Exploring Data Integration Frontiers: Airbyte and Data Load Tool (DLT)](https://medium.com/odicis-data-engineering/exploring-data-integration-frontiers-airbyte-and-data-load-tool-dlt-b882446bea23)

3. [How to build ETL pipeline with Incremental Data Load with Python | Python | ETL](https://www.youtube.com/watch?v=a_T8xRaCO60)

4. [Incremental processing for Heavy Bulk API and DML use cases](https://help.salesforce.com/s/articleView?id=000382007&type=1)

5. [sObject Get Updated](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_getupdated.htm)

6. [Python Code Optimization](https://youtu.be/LI5O6rfe7zI?si=Bke_Qc7MQEsRHvdJ)