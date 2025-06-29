{{ config(materialized='table') }}

with payment_metrics as (
  select
    p.payment_desc,
    p.payment_type,
    count(*) as trip_count,
    round(sum(f.total_amount), 2) as total_revenue,
    round(avg(f.total_amount), 2) as avg_fare,
    round(sum(f.tip_amount), 2) as total_tips,
    round(avg(f.tip_amount), 2) as avg_tip,
    -- Tips are only recorded for credit card payments
    round(avg(case when p.payment_type = 1 then f.tip_amount else null end), 2) as avg_credit_card_tip,
    round(avg(f.trip_distance), 2) as avg_distance,
    round(avg(f.trip_duration_min), 2) as avg_duration
  from {{ ref('fact_yellow_trip') }} f
  inner join {{ ref('dim_payment') }} p on f.payment_type = p.payment_type
  group by 1, 2
)

select
  payment_desc,
  payment_type,
  
  -- Volume metrics
  trip_count,
  round(100.0 * trip_count / sum(trip_count) over (), 2) as pct_of_trips,
  dense_rank() over (order by trip_count desc) as popularity_rank,
  
  -- Revenue metrics
  total_revenue,
  round(100.0 * total_revenue / sum(total_revenue) over (), 2) as pct_of_revenue,
  avg_fare,
  
  -- Tip analysis (meaningful for credit cards)
  case 
    when payment_type = 1 then total_tips
    else 0
  end as total_tips,
  case 
    when payment_type = 1 then avg_credit_card_tip
    else 0
  end as avg_tip,
  case 
    when payment_type = 1 and avg_fare > 0 
    then round(100.0 * avg_credit_card_tip / avg_fare, 2)
    else 0
  end as tip_pct,
  
  -- Operational metrics
  avg_distance,
  avg_duration,
  
  -- Revenue per mile
  case 
    when avg_distance > 0 
    then round(avg_fare / avg_distance, 2)
    else null
  end as revenue_per_mile

from payment_metrics
order by total_revenue desc