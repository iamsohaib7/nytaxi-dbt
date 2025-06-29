{{ config(materialized='table') }}

with daily_metrics as (
  select
    f.trip_date,
    d.date,
    d.day_name,
    d.day_of_week,
    d.is_weekend,
    d.month_name,
    d.year,
    d.quarter,
    count(*) as total_trips,
    round(sum(f.total_amount), 2) as total_revenue,
    round(avg(f.total_amount), 2) as avg_fare,
    round(avg(f.trip_distance), 2) as avg_distance,
    round(avg(f.trip_duration_min), 2) as avg_duration_min,
    round(sum(f.tip_amount), 2) as total_tips,
    round(avg(f.tip_amount), 2) as avg_tip,
    count(distinct f.pulocationid) as unique_pickup_zones,
    count(distinct f.dolocationid) as unique_dropoff_zones,
    round(min(f.total_amount), 2) as min_fare,
    round(max(f.total_amount), 2) as max_fare
  from {{ ref('fact_yellow_trip') }} f
  inner join {{ ref('dim_date') }} d on f.trip_date = d.date_id
  group by 1, 2, 3, 4, 5, 6, 7, 8
)

select
  *,
  -- Calculate moving averages
  round(avg(total_trips) over (
    order by trip_date 
    rows between 6 preceding and current row
  ), 0) as trips_7day_avg,
  
  round(avg(total_revenue) over (
    order by trip_date 
    rows between 6 preceding and current row
  ), 2) as revenue_7day_avg,
  
  -- Day-over-day changes
  lag(total_trips, 1) over (order by trip_date) as prev_day_trips,
  lag(total_revenue, 1) over (order by trip_date) as prev_day_revenue,
  
  round(100.0 * (total_trips - lag(total_trips, 1) over (order by trip_date))::float / 
        nullif(lag(total_trips, 1) over (order by trip_date), 0), 1) as trips_dod_change_pct,
        
  -- Week-over-week comparison (same day last week)
  lag(total_trips, 7) over (order by trip_date) as same_day_last_week_trips,
  
  round(100.0 * (total_trips - lag(total_trips, 7) over (order by trip_date))::float / 
        nullif(lag(total_trips, 7) over (order by trip_date), 0), 1) as trips_wow_change_pct,
        
  -- Revenue per trip
  round(total_revenue / nullif(total_trips, 0), 2) as revenue_per_trip

from daily_metrics
order by trip_date