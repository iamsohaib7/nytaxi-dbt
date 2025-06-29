{{ config(materialized='table') }}

select
  p.payment_desc,
  count(*) as trip_count,
  round(sum(f.total_amount), 2) as total_revenue,
  round(avg(f.total_amount), 2) as avg_revenue_per_trip
from {{ ref('fact_yellow_trip') }} f
left join {{ ref('dim_payment') }} p on f.payment_type = p.payment_type
group by 1
order by total_revenue desc
