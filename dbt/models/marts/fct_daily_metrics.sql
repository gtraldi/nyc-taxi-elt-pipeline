{{ config (
		materialized='incremental',
		unique_key=['vendor_id', 'pickup_date'],
		incremental_strategy='delete+insert'
	)
}}

with enriched_trips_data as (
	select * from {{ ref('int_nyc_yellow_trip_enriched') }} 
	{% if is_incremental() %}
		where pickup_datetime::date >= (select max(pickup_date) from {{ this }})
	{% endif %}
),
prepared_trips_duration as (
	select
		*,
		extract(epoch from (dropoff_datetime - pickup_datetime)) / 60 as duration_minutes
	from enriched_trips_data
),
daily_metrics as (
	select
		vendor_id,
		vendor_name,
		cast(pickup_datetime as date) as pickup_date,
		count(*) as total_trips,
		sum(passenger_count) as total_passengers,
		cast(sum(trip_distance) as numeric(12,2)) as total_distance,
		cast(sum(total_amount) as numeric(12,2)) as total_revenue,
		cast(avg(total_amount) as numeric(12,2)) as avg_fare_per_trip,
		sum(tip_amount) as total_tips,
		cast(avg(tip_amount) as numeric(12,2)) as avg_tip_amount,
		cast(avg(trip_distance) as numeric(12,2)) as avg_trip_distance,
		cast(sum(tip_amount) / nullif(sum(fare_amount), 0) * 100 as numeric(12,2)) as tip_percentage,
		round(avg(duration_minutes)) as avg_duration_minutes
	from prepared_trips_duration
	group by vendor_id, pickup_date, vendor_name
)

select * from daily_metrics
