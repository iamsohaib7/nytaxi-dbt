{{ config(materialized='table') }}

select
  LocationID as location_id,
  Borough,
  Zone as zone_name,
  service_zone
from {{ ref('taxi_zone_lookup') }}
