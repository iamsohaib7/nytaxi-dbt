{{ config(materialized='table') }}

with hourly_stats as (
  select
    f.pickup_hour,
    d.is_weekend,
    count(*) as trip_count,
    round(avg(f.total_amount), 2) as avg_fare,
    round(avg(f.trip_distance), 2) as avg_distance,
    round(avg(f.trip_duration_min), 2) as avg_duration,
    round(avg(f.tip_amount), 2) as avg_tip,
    round(sum(f.total_amount), 2) as total_revenue,
    count(distinct f.trip_date) as days_with_trips
  from {{ ref('fact_yellow_trip') }} f
  inner join {{ ref('dim_date') }} d on f.trip_date = d.date_id
  group by 1, 2
),
hourly_summary as (
  select
    pickup_hour,
    sum(case when is_weekend = false then trip_count else 0 end) as weekday_trips,
    sum(case when is_weekend = true then trip_count else 0 end) as weekend_trips,
    sum(trip_count) as total_trips,
    avg(case when is_weekend = false then avg_fare else null end) as weekday_avg_fare,
    avg(case when is_weekend = true then avg_fare else null end) as weekend_avg_fare,
    avg(avg_fare) as overall_avg_fare,
    avg(avg_distance) as avg_distance,
    avg(avg_duration) as avg_duration,
    sum(total_revenue) as total_revenue
  from hourly_stats
  group by 1
)

select
  pickup_hour,
  case
    when pickup_hour between 6 and 9 then 'Morning Rush'
    when pickup_hour between 17 and 19 then 'Evening Rush'
    when pickup_hour between 10 and 16 then 'Midday'
    when pickup_hour between 20 and 23 then 'Night'
    else 'Late Night/Early Morning'
  end as time_period,
  
  -- Trip volumes
  total_trips,
  weekday_trips,
  weekend_trips,
  round(100.0 * total_trips / sum(total_trips) over (), 2) as pct_of_daily_trips,
  
  -- Classify as peak hour (top 25% of traffic)
  case 
    when total_trips >= percentile_cont(0.75) within group (order by total_trips) over ()
    then true
    else false
  end as is_peak_hour,
  
  -- Financial metrics
  round(overall_avg_fare, 2) as avg_fare,
  round(weekday_avg_fare, 2) as weekday_avg_fare,
  round(weekend_avg_fare, 2) as weekend_avg_fare,
  round(total_revenue, 2) as total_revenue,
  round(100.0 * total_revenue / sum(total_revenue) over (), 2) as pct_of_daily_revenue,
  
  -- Operational metrics
  round(avg_distance, 2) as avg_distance,
  round(avg_duration, 2) as avg_duration,
  
  -- Demand intensity (0-1 scale)
  round(total_trips::float / max(total_trips) over (), 3) as demand_intensity

from hourly_summary
order by pickup_hour