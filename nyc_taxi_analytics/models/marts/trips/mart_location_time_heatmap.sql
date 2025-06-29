{{ config(materialized='table') }}

select
  pu.zone_name as pickup_zone,
  f.pickup_hour,
  count(*) as trip_count
from {{ ref('fact_yellow_trip') }} f
left join {{ ref('dim_location_zone') }} pu on f.pulocationid = pu.location_id
group by 1, 2
order by 1, 2
