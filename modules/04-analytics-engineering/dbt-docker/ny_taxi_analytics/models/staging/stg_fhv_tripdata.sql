{{ config(materialized='table') }}
 
with tripdata as 
(
  select *,
  -- row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
  from {{ source('crude','ext_fhv_tripdata') }}
  where dispatching_base_num is not null 
  and EXTRACT(YEAR from pickup_datetime) = 2019
)
select
   -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,    
    dispatching_base_num,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    affiliated_base_number,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    sr_flag
from tripdata
-- where rn = 1

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}