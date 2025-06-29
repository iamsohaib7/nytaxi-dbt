{{ config(materialized='table') }}

with fare_categorized as (
  select
    *,
    case
      when total_amount < 10 then 1
      when total_amount < 20 then 2
      when total_amount < 30 then 3
      when total_amount < 40 then 4
      else 5
    end as fare_bin_order,
    case
      when total_amount < 10 then '< $10'
      when total_amount < 20 then '$10-19'
      when total_amount < 30 then '$20-29'
      when total_amount < 40 then '$30-39'
      else '$40+'
    end as fare_bin
  from {{ ref('fact_yellow_trip') }}
  where total_amount >= 0  -- Exclude negative fares
)

select
  fare_bin,
  fare_bin_order,
  count(*) as trip_count,
  round(100.0 * count(*) / sum(count(*)) over(), 2) as pct_of_trips,
  round(sum(total_amount), 2) as total_revenue,
  round(100.0 * sum(total_amount) / sum(sum(total_amount)) over(), 2) as pct_of_revenue,
  round(avg(total_amount), 2) as avg_fare,
  round(avg(trip_distance), 2) as avg_distance_miles,
  round(avg(trip_duration_min), 2) as avg_duration_min,
  round(avg(tip_amount), 2) as avg_tip,
  round(avg(case when trip_distance > 0 then total_amount / trip_distance else null end), 2) as avg_fare_per_mile
from fare_categorized
group by 1, 2
order by fare_bin_order