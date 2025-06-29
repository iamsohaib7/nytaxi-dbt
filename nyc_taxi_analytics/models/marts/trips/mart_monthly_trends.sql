{{ config(materialized='table') }}

select
  date_trunc('month', pickup_ts) as month,
  count(*) as total_trips,
  round(sum(total_amount), 2) as total_revenue,
  round(avg(total_amount), 2) as avg_fare
from {{ ref('fact_yellow_trip') }}
group by 1
order by 1
