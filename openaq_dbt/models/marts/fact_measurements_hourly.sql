{{ config(
    materialized = 'table',
    partition_by = {
      "field": "datetime_utc",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

select
    m.sensor_id,
    m.location_id,
    d.location_name,
    d.country,
    d.latitude,
    d.longitude,
    m.parameter_id,
    m.parameter_name,
    m.units,
    m.datetime_utc,
    m.value
from {{ ref('stg_measurements_hours') }} as m
left join {{ ref('dim_locations') }} as d
  on m.location_id = d.location_id
where m.datetime_utc is not null
  and m.value is not null
