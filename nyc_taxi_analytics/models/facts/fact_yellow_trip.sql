{{ config(
    materialized='incremental',
    unique_key='trip_id'
) }}

with staged as (
    select
        -- Unique identifier for the trip
        md5(
            concat(
                coalesce(tpep_pickup_datetime, ''),
                coalesce(cast(passenger_count as string), ''),
                coalesce(cast(pulocationid as string), ''),
                coalesce(cast(dolocationid as string), ''),
                coalesce(cast(fare_amount as string), '')
            )
        ) as trip_id,

        -- Time dimensions
        date(try_to_timestamp(tpep_pickup_datetime)) as trip_date,
        try_to_timestamp(tpep_pickup_datetime) as pickup_ts,
        try_to_timestamp(tpep_dropoff_datetime) as dropoff_ts,
        extract(hour from try_to_timestamp(tpep_pickup_datetime)) as pickup_hour,
        dayofweek(try_to_timestamp(tpep_pickup_datetime)) as pickup_weekday_num,
        datediff('minute', try_to_timestamp(tpep_pickup_datetime), try_to_timestamp(tpep_dropoff_datetime)) as trip_duration_min,

        -- Core trip data
        passenger_count,
        trip_distance,

        -- Dimensions (FKs)
        ratecodeid,
        payment_type,
        pulocationid,
        dolocationid,

        -- Financial metrics
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        total_amount

    from {{ ref('stg_yellow_tripdata') }}
)

select *
from staged

{% if is_incremental() %}
  where try_to_timestamp(tpep_pickup_datetime) > (
      select max(pickup_ts) from {{ this }}
  )
{% endif %}
