{{ config(materialized='table') }}

select
  pu.zone_name as pickup_zone,
  do.zone_name as dropoff_zone,
  count(*) as trip_count,
  round(avg(f.total_amount), 2) as avg_total_amount,
  round(avg(f.trip_distance), 2) as avg_distance
from {{ ref('fact_yellow_trip') }} f
left join {{ ref('dim_location_zone') }} pu on f.pulocationid = pu.location_id
left join {{ ref('dim_location_zone') }} do on f.dolocationid = do.location_id
group by 1, 2
order by trip_count desc
limit 100
