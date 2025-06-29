{{ config(materialized='table') }}

with route_metrics as (
  select
    pu.zone_name as pickup_zone,
    do.zone_name as dropoff_zone,
    pu.borough as pickup_borough,
    do.borough as dropoff_borough,
    count(*) as trip_count,
    round(avg(f.total_amount), 2) as avg_total_amount,
    round(avg(f.trip_distance), 2) as avg_distance_miles,
    round(avg(f.trip_duration_min), 2) as avg_duration_min,
    round(sum(f.total_amount), 2) as total_revenue,
    round(avg(f.tip_amount), 2) as avg_tip,
    round(stddev(f.total_amount), 2) as fare_stddev,
    round(min(f.total_amount), 2) as min_fare,
    round(max(f.total_amount), 2) as max_fare,
    -- Calculate if it's an inter-borough trip
    case 
      when pu.borough != do.borough then 'Inter-Borough'
      else 'Same-Borough'
    end as trip_type
  from {{ ref('fact_yellow_trip') }} f
  inner join {{ ref('dim_location_zone') }} pu on f.pulocationid = pu.location_id
  inner join {{ ref('dim_location_zone') }} do on f.dolocationid = do.location_id
  group by 1, 2, 3, 4
  having count(*) >= 10  -- Only include routes with meaningful volume
)

select 
  *,
  round(100.0 * trip_count / sum(trip_count) over(), 2) as pct_of_total_trips,
  round(100.0 * total_revenue / sum(total_revenue) over(), 2) as pct_of_total_revenue,
  dense_rank() over (order by trip_count desc) as popularity_rank,
  dense_rank() over (order by total_revenue desc) as revenue_rank
from route_metrics
order by trip_count desc
limit 100