{{ config(materialized='table') }}

select
  pu.zone_name as pickup_zone,
  count(*) as trip_count,
  round(sum(total_amount), 2) as total_revenue,
  round(avg(total_amount), 2) as avg_fare
from {{ ref('fact_yellow_trip') }} f
left join {{ ref('dim_location_zone') }} pu on f.pulocationid = pu.location_id
group by 1
order by total_revenue desc
limit 20
