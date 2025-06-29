{{ config(materialized='table') }}

SELECT
  DISTINCT
  DATE(TRY_TO_TIMESTAMP(tpep_pickup_datetime)) AS date,
  EXTRACT(dayofweek FROM TRY_TO_TIMESTAMP(tpep_pickup_datetime)) AS weekday_num,
  DECODE(
    EXTRACT(dayofweek FROM TRY_TO_TIMESTAMP(tpep_pickup_datetime)),
    1, 'Sunday',
    2, 'Monday',
    3, 'Tuesday',
    4, 'Wednesday',
    5, 'Thursday',
    6, 'Friday',
    7, 'Saturday'
  ) AS weekday_name,
  DECODE(
    EXTRACT(dayofweek FROM TRY_TO_TIMESTAMP(tpep_pickup_datetime)),
    1, TRUE,
    7, TRUE,
    FALSE
  ) AS is_weekend
FROM {{ ref('stg_yellow_tripdata') }}
