{{ config(
    materialized='incremental',
    unique_key='trip_id',
    on_schema_change='sync_all_columns'
) }}

with staged as (
    select
        -- Generate sequential ID within this batch
        row_number() over (
            order by 
                try_to_timestamp(tpep_pickup_datetime),
                vendorid,
                pulocationid,
                dolocationid,
                passenger_count
        ) as batch_row_num,
        
        -- Time dimensions
        date(try_to_timestamp(tpep_pickup_datetime)) as trip_date,
        try_to_timestamp(tpep_pickup_datetime) as pickup_ts,
        try_to_timestamp(tpep_dropoff_datetime) as dropoff_ts,
        extract(hour from try_to_timestamp(tpep_pickup_datetime)) as pickup_hour,
        dayofweek(try_to_timestamp(tpep_pickup_datetime)) as pickup_weekday_num,
        datediff('minute', try_to_timestamp(tpep_pickup_datetime), try_to_timestamp(tpep_dropoff_datetime)) as trip_duration_min,
        
        -- Core trip data
        vendorid,
        passenger_count,
        trip_distance,
        store_and_fwd_flag,
        
        -- Dimensions (FKs)
        ratecodeid,
        payment_type,
        pulocationid,  -- Keep original names
        dolocationid,  -- Keep original names
        
        -- Financial metrics
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        total_amount,
        
        -- Audit column
        current_timestamp() as dbt_loaded_at
        
    from {{ ref('stg_yellow_tripdata') }}
    where tpep_pickup_datetime is not null
    
    {% if is_incremental() %}
        and try_to_timestamp(tpep_pickup_datetime) > (
            select max(pickup_ts) from {{ this }}
        )
    {% endif %}
)

select 
    -- Create globally unique trip_id
    {% if is_incremental() %}
        (select coalesce(max(trip_id), 0) from {{ this }}) + batch_row_num as trip_id,
    {% else %}
        batch_row_num as trip_id,
    {% endif %}
    * exclude(batch_row_num)
from staged