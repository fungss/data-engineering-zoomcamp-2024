# Module 4 - Analytics Engineering
- Model data following kimball style (star schema)
- Autogenerate schema.yml with dbt-labs/codegen
- Perform data validation with dbt
- Use of dbt project variables
- Generate documentation (Schema, Lineage) with dbt

## Notes
1. dbt is a transformation tool that facilitates software engineering best practices like modularity, portability, CI/CD, and documentation.
2. Views
 - abstract complex joins from user
 - limit degree of exposure - avoid giving table level permissions and faciliate hiding of PII info
 - allow changing logic and behavior without changing the output structure
 - In dbt, materialized view automatically updates (due to caching in database), while table does not.
 - Views vs tables - depending on whether users need the flexibility to make adhoc query
3. dbt tests can be used for data validation (uniqueness, nullity, accepted values, foreign key, and custom checks)
4. To access dbt UI, especially for the lineage graph,
```
docker compose run dbt-bq docs generate
docker compose run --service-ports dbt-bq docs serve
```

## Preparation
Assuming GCP Cloud Storage and related resources are provided via the terraform script, run the below items to
1. extract and load data to GCS
2. create external table in the crude layer of Data Warehouse (Bigquery)
3. materialize the data models in Bigquery via dbt
```
python web-to-gcs-etl.py
docker compose build --no-cache
docker compose run dbt-bq build --vars '{is_test_run: false}'
```

## Assignment
Questions can be found [here](https://github.com/fungss/data-engineering-zoomcamp-2024/blob/main/modules/04-analytics-engineering/homework.md).

## Question 1 - dbt with custom variables
Answer: It applies a limit 100 only to our staging models

## Question 2 - CI job
Answer: The code from the development branch we are requesting to merge to main

## Question 3 - Record count in model fact_fhv
```
select count(*)
from `dtc-de-course-410021.core.fct_fhv_trips`
```
Answer: 22998722

## Question 4 - Service with the most rides in July 2019
```
select service_type, count(*) as jul_2019_cnt
-- select pickup_datetime
from `dtc-de-course-410021.core.fct_fhv_trips`
where EXTRACT(YEAR from pickup_datetime) = 2019
and EXTRACT(MONTH from pickup_datetime) = 7
group by service_type

union all

select service_type, count(*) as jul_2019_cnt
-- select pickup_datetime
from `dtc-de-course-410021.core.fct_trips`
where EXTRACT(YEAR from pickup_datetime) = 2019
and EXTRACT(MONTH from pickup_datetime) = 7
group by service_type
```
Answer: Yellow

## Reference
1. [SQL Tables and Views: What's the Difference?](https://youtu.be/eumDqVqaCT4?si=lN2-CONmXa_kEDHc)

2. [When to use a View instead of a Table?](https://stackoverflow.com/questions/4378068/when-to-use-a-view-instead-of-a-table)

3. [Materializations](https://docs.getdbt.com/docs/build/materializations)

4. [Materialized View vs Table Using dbt](https://stackoverflow.com/questions/64489772/materialized-view-vs-table-using-dbt#:~:text=If%20you%20have%20a%20DBT,the%20table%20by%20scheduling%20DBT.)

5. [Materialized View vs. Tables: What are the advantages?](https://stackoverflow.com/questions/4218657/materialized-view-vs-tables-what-are-the-advantages)

6. [Anything one should know before going for self-hosted dbt?](https://www.reddit.com/r/dataengineering/comments/14w832y/anything_one_should_know_before_going_for/)

7. [Finally, a better way to deploy DBT on Google Cloud!](https://medium.com/@matthh9797/finally-a-better-way-to-deploy-dbt-on-google-cloud-583b540ecf69)

8. [FAQ: Cleaning up removed models from your production schema](https://discourse.getdbt.com/t/faq-cleaning-up-removed-models-from-your-production-schema/113)

9. [Deploy to custom schemas & override dbt defaults](https://youtu.be/AvrVQr5FHwk?si=X3tHCY2CabiJ1zdK)

10. [datnguye/dbterd](https://github.com/datnguye/dbterd)

11. [Building OLAP Dimensional Model in BigQuery, using dbt as a Data Transformation Tool.
](https://github.com/Chisomnwa/Building-OLAP-Dimensional-Model-using-BigQuery-and-DBT)