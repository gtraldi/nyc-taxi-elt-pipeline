with staging as (
	select * from {{ ref('stg_nyc_yellow_trip') }} 
),
prepared_trips_duration as (
	select
		*,
		extract(epoch from (dropoff_datetime - pickup_datetime)) / 60 as duration_minutes
	from staging
	where dropoff_datetime > pickup_datetime
),
monthly_metrics as (
	select
		vendor_id,
		count(*) as total_trips,
		sum(passenger_count) as total_passengers,
		cast(sum(trip_distance) as numeric(12,2)) as total_distance,
		cast(sum(total_amount) as numeric(12,2)) as total_revenue,
		cast(avg(total_amount) as numeric(12,2)) as avg_fare_per_trip,
		sum(tip_amount) as total_tips,
		cast(avg(tip_amount) as numeric(12,2)) as avg_tip_amount,
		cast(avg(trip_distance) as numeric(12,2)) as avg_trip_distance,
		cast(sum(tip_amount) / nullif(sum(fare_amount), 0) * 100 as numeric(12,2)) as tip_percentage,
		round(avg(duration_minutes)) as avg_duration_minutes,
		concat(file_year, '-', lpad(file_month::text, 2, '0')) as year_month
	from prepared_trips_duration
	group by vendor_id, year_month
)

select * from monthly_metrics