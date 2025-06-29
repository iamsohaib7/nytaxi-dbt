{{ config(materialized='table') }}

with monthly_metrics as (
  select
    date_trunc('month', pickup_ts) as month,
    count(*) as total_trips,
    round(sum(total_amount), 2) as total_revenue,
    round(avg(total_amount), 2) as avg_fare,
    round(avg(trip_distance), 2) as avg_distance,
    round(avg(trip_duration_min), 2) as avg_duration,
    round(sum(tip_amount), 2) as total_tips,
    round(avg(tip_amount), 2) as avg_tip,
    count(distinct trip_date) as operating_days,
    count(distinct pulocationid) as unique_pickup_locations,
    count(distinct dolocationid) as unique_dropoff_locations
  from {{ ref('fact_yellow_trip') }}
  group by 1
)

select
  month,
  extract(year from month) as year,
  extract(month from month) as month_num,
  to_char(month, 'YYYY-MM') as year_month,
  to_char(month, 'Month') as month_name,
  
  -- Core metrics
  total_trips,
  total_revenue,
  avg_fare,
  avg_distance,
  avg_duration,
  total_tips,
  avg_tip,
  
  -- Calculated metrics
  round(total_trips::float / operating_days, 0) as avg_daily_trips,
  round(total_revenue / operating_days, 2) as avg_daily_revenue,
  unique_pickup_locations,
  unique_dropoff_locations,
  
  -- Month-over-month changes
  lag(total_trips) over (order by month) as prev_month_trips,
  lag(total_revenue) over (order by month) as prev_month_revenue,
  
  round(100.0 * (total_trips - lag(total_trips) over (order by month))::float / 
        nullif(lag(total_trips) over (order by month), 0), 1) as trips_mom_change_pct,
        
  round(100.0 * (total_revenue - lag(total_revenue) over (order by month))::float / 
        nullif(lag(total_revenue) over (order by month), 0), 1) as revenue_mom_change_pct,
        
  -- Running totals
  sum(total_trips) over (order by month) as cumulative_trips,
  sum(total_revenue) over (order by month) as cumulative_revenue
  
from monthly_metrics
order by month