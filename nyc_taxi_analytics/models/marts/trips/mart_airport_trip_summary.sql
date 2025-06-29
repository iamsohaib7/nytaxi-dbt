{{ config(materialized='table') }}

select
  case
    when pu.zone_name ilike '%airport%' or do.zone_name ilike '%airport%' then 'Airport Trip'
    else 'Non-Airport Trip'
  end as trip_type,
  count(*) as trip_count,
  round(avg(total_amount), 2) as avg_fare,
  round(avg(trip_distance), 2) as avg_distance,
  round(avg(trip_duration_min), 2) as avg_duration
from {{ ref('fact_yellow_trip') }} f
left join {{ ref('dim_location_zone') }} pu on f.pulocationid = pu.location_id
left join {{ ref('dim_location_zone') }} do on f.dolocationid = do.location_id
group by 1
