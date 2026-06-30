select *
from {{ ref('stg_nyc_yellow_trip') }}
where dropoff_datetime <= pickup_datetime