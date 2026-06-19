with source as (
    select * from {{ source('raw_nyc_data', 'nyc_yellow_trip') }}
),
renamed as (
    select
        vendorid as vendor_id,
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as numeric(10,2)) as trip_distance,
        cast(ratecodeid as integer) as ratecode_id,
        store_and_fwd_flag,
        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,
        cast(payment_type as integer) as payment_type,
        cast(fare_amount as numeric(10,2)) as fare_amount,
        cast(extra as numeric(10,2)) as extra,
        cast(mta_tax as numeric(10,2)) as mta_tax,
        cast(tip_amount as numeric(10,2)) as tip_amount,
        cast(tolls_amount as numeric(10,2)) as tolls_amount,
        cast(improvement_surcharge as numeric(10,2)) as improvement_surcharge,
        cast(total_amount as numeric(10,2)) as total_amount,
        cast(congestion_surcharge as numeric(10,2)) as congestion_surcharge,
        cast(airport_fee as numeric(10,2)) as airport_fee,
        cast(cbd_congestion_fee as numeric(10,2)) as cbd_congestion_fee,
        loaded_at,
        file_year,
        file_month
    from source
    where trip_distance >= 0
        and total_amount >= 0
        and extract(year from tpep_pickup_datetime) = file_year
        and extract(month from tpep_pickup_datetime) = file_month
)

select * from renamed
