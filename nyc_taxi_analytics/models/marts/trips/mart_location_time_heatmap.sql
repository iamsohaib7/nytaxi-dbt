{{ config(materialized='table') }}

with hourly_trips as (
  select
    pu.zone_name as pickup_zone,
    pu.borough,
    f.pickup_hour,
    d.is_weekend,
    count(*) as trip_count
  from {{ ref('fact_yellow_trip') }} f
  inner join {{ ref('dim_location_zone') }} pu on f.pulocationid = pu.location_id
  inner join {{ ref('dim_date') }} d on f.trip_date = d.date_id
  group by 1, 2, 3, 4
),
zone_totals as (
  select
    pickup_zone,
    sum(trip_count) as zone_total_trips
  from hourly_trips
  group by 1
  having sum(trip_count) >= 100  -- Filter out low-volume zones
)

select
  h.pickup_zone,
  h.borough,
  h.pickup_hour,
  h.is_weekend,
  h.trip_count,
  zt.zone_total_trips,
  round(100.0 * h.trip_count / zt.zone_total_trips, 2) as pct_of_zone_trips,
  -- Identify peak hours for each zone
  case 
    when h.trip_count >= percentile_cont(0.9) within group (order by h.trip_count) 
         over (partition by h.pickup_zone) 
    then true 
    else false 
  end as is_peak_hour,
  -- Calculate hour intensity (0-1 scale for heatmap coloring)
  round(h.trip_count::float / max(h.trip_count) over (partition by h.pickup_zone), 3) as hour_intensity
from hourly_trips h
inner join zone_totals zt on h.pickup_zone = zt.pickup_zone
order by h.pickup_zone, h.is_weekend, h.pickup_hour