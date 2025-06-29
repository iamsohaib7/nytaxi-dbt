{{ config(materialized='table') }}

select
  f.trip_date,
  count(*) as total_trips,
  sum(f.total_amount) as total_revenue,
  avg(f.trip_distance) as avg_distance,
  avg(f.trip_duration_min) as avg_duration_min,
  d.weekday_name,
  d.is_weekend
from {{ ref('fact_yellow_trip') }} f
left join {{ ref('dim_date') }} d on f.trip_date = d.date
group by 1, 6, 7
order by 1
