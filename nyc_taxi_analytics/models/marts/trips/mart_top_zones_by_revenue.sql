{{ config(materialized='table') }}

with zone_metrics as (
  select
    pu.zone_name as pickup_zone,
    pu.borough,
    pu.service_zone,
    count(*) as trip_count,
    round(sum(f.total_amount), 2) as total_revenue,
    round(avg(f.total_amount), 2) as avg_fare,
    round(avg(f.trip_distance), 2) as avg_distance,
    round(avg(f.trip_duration_min), 2) as avg_duration,
    round(sum(f.tip_amount), 2) as total_tips,
    round(avg(f.tip_amount), 2) as avg_tip,
    count(distinct f.trip_date) as active_days,
    count(distinct f.dolocationid) as unique_destinations
  from {{ ref('fact_yellow_trip') }} f
  inner join {{ ref('dim_location_zone') }} pu on f.pulocationid = pu.location_id
  group by 1, 2, 3
)

select
  pickup_zone,
  borough,
  service_zone,
  
  -- Volume metrics
  trip_count,
  round(100.0 * trip_count / sum(trip_count) over (), 2) as pct_of_total_trips,
  
  -- Revenue metrics
  total_revenue,
  round(100.0 * total_revenue / sum(total_revenue) over (), 2) as pct_of_total_revenue,
  avg_fare,
  
  -- Rankings
  row_number() over (order by total_revenue desc) as revenue_rank,
  row_number() over (order by trip_count desc) as volume_rank,
  row_number() over (order by avg_fare desc) as avg_fare_rank,
  
  -- Operational insights
  avg_distance,
  avg_duration,
  round(total_revenue / active_days, 2) as avg_daily_revenue,
  round(trip_count::float / active_days, 1) as avg_daily_trips,
  unique_destinations,
  
  -- Tip metrics
  avg_tip,
  round(100.0 * avg_tip / nullif(avg_fare, 0), 2) as tip_pct

from zone_metrics
order by total_revenue desc
limit 20