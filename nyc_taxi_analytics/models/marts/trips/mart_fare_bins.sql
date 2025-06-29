{{ config(materialized='table') }}

select
  case
    when total_amount < 10 then '< $10'
    when total_amount < 20 then '$10-19'
    when total_amount < 30 then '$20-29'
    when total_amount < 40 then '$30-39'
    else '$40+'
  end as fare_bin,
  count(*) as trip_count,
  round(avg(trip_distance), 2) as avg_distance,
  round(avg(trip_duration_min), 2) as avg_duration
from {{ ref('fact_yellow_trip') }}
group by 1
order by 1
