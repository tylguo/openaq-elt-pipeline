{{ config(materialized='view') }}

select
  sensor_id,
  location_id,
  country,
  latitude,
  longitude,
  parameter_id,
  parameter_name,
  units,
  datetime_utc,
  value
from {{ source('openaq_raw', 'measurements_hours_p') }}
qualify
  row_number() over (
    partition by sensor_id, datetime_utc
    order by datetime_utc desc
  ) = 1
