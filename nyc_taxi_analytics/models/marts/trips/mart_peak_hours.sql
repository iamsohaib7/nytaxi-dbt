{{ config(materialized='table') }}

select
  pickup_hour,
  count(*) as trip_count,
  round(avg(total_amount), 2) as avg_total,
  round(avg(trip_distance), 2) as avg_distance
from {{ ref('fact_yellow_trip') }}
group by pickup_hour
order by pickup_hour
