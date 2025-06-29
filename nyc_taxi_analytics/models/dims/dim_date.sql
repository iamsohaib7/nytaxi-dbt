{{ config(materialized='table') }}

SELECT DISTINCT
  DATE(TRY_TO_TIMESTAMP(tpep_pickup_datetime)) AS date_id,
  DATE(TRY_TO_TIMESTAMP(tpep_pickup_datetime)) AS date,
  
  -- Day attributes
  DAYOFWEEK(DATE(TRY_TO_TIMESTAMP(tpep_pickup_datetime))) AS day_of_week,
  DAYNAME(DATE(TRY_TO_TIMESTAMP(tpep_pickup_datetime))) AS day_name,
  DAY(DATE(TRY_TO_TIMESTAMP(tpep_pickup_datetime))) AS day_of_month,
  DAYOFYEAR(DATE(TRY_TO_TIMESTAMP(tpep_pickup_datetime))) AS day_of_year,
  
  -- Week attributes
  WEEKOFYEAR(DATE(TRY_TO_TIMESTAMP(tpep_pickup_datetime))) AS week_of_year,
  CASE 
    WHEN DAYOFWEEK(DATE(TRY_TO_TIMESTAMP(tpep_pickup_datetime))) IN (0, 6) THEN TRUE
    ELSE FALSE
  END AS is_weekend,
  
  -- Month attributes
  MONTH(DATE(TRY_TO_TIMESTAMP(tpep_pickup_datetime))) AS month_num,
  MONTHNAME(DATE(TRY_TO_TIMESTAMP(tpep_pickup_datetime))) AS month_name,
  
  -- Quarter and Year
  QUARTER(DATE(TRY_TO_TIMESTAMP(tpep_pickup_datetime))) AS quarter,
  YEAR(DATE(TRY_TO_TIMESTAMP(tpep_pickup_datetime))) AS year

FROM {{ ref('stg_yellow_tripdata') }}
WHERE tpep_pickup_datetime IS NOT NULL