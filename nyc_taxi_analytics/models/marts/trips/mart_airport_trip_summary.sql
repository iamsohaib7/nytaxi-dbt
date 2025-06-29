{{ config(materialized='table') }}

with trip_categorized as (
  select
    f.*,
    case
      when pu.zone_name ilike '%airport%' or do.zone_name ilike '%airport%' 
      then 'Airport Trip'
      else 'Non-Airport Trip'
    end as trip_type,
    case
      when pu.zone_name ilike '%JFK%' or do.zone_name ilike '%JFK%' then 'JFK'
      when pu.zone_name ilike '%LaGuardia%' or do.zone_name ilike '%LaGuardia%' then 'LaGuardia'
      when pu.zone_name ilike '%Newark%' or do.zone_name ilike '%Newark%' then 'Newark'
      when pu.zone_name ilike '%airport%' or do.zone_name ilike '%airport%' then 'Other Airport'
      else 'Non-Airport'
    end as airport_name
  from {{ ref('fact_yellow_trip') }} f
  inner join {{ ref('dim_location_zone') }} pu on f.pulocationid = pu.location_id
  inner join {{ ref('dim_location_zone') }} do on f.dolocationid = do.location_id
)

select
  trip_type,
  airport_name,
  count(*) as trip_count,
  round(avg(total_amount), 2) as avg_fare,
  round(avg(trip_distance), 2) as avg_distance_miles,
  round(avg(trip_duration_min), 2) as avg_duration_min,
  round(avg(tip_amount), 2) as avg_tip,
  round(avg(case when payment_type = 1 then tip_amount else 0 end), 2) as avg_credit_card_tip,
  round(sum(total_amount), 2) as total_revenue,
  round(100.0 * count(*) / sum(count(*)) over(), 2) as pct_of_all_trips
from trip_categorized
group by 1, 2
order by 1, 2