{{ config(materialized='view') }}
select
  vendorid,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  ratecodeid,
  store_and_fwd_flag,
  pulocationid,  
  dolocationid,  
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  airport_fee
from {{ source('nyc_yellow', 'nyc_yellow_taxi_trips_raw_data') }}