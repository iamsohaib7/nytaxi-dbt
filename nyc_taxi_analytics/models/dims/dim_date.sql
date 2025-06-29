{{ config(materialized='table') }}

select
  distinct 
  date(try_to_timestamp(tpep_pickup_datetime)) as date,
  dayofweek(try_to_timestamp(tpep_pickup_datetime)) as weekday_num,
  case 
    when dayofweek(try_to_timestamp(tpep_pickup_datetime)) = 1 then 'Sunday'
    when dayofweek(try_to_timestamp(tpep_pickup_datetime)) = 2 then 'Monday'
    when dayofweek(try_to_timestamp(tpep_pickup_datetime)) = 3 then 'Tuesday'
    when dayofweek(try_to_timestamp(tpep_pickup_datetime)) = 4 then 'Wednesday'
    when dayofweek(try_to_timestamp(tpep_pickup_datetime)) = 5 then 'Thursday'
    when dayofweek(try_to_timestamp(tpep_pickup_datetime)) = 6 then 'Friday'
    when dayofweek(try_to_timestamp(tpep_pickup_datetime)) = 7 then 'Saturday'
  end as weekday_name,
  case 
    when dayofweek(try_to_timestamp(tpep_pickup_datetime)) in (1, 7) then true
    else false
  end as is_weekend
from {{ ref('stg_yellow_tripdata') }}
