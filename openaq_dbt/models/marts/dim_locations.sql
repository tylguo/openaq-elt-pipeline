{{
    config(
        materialized = 'table'
) }}

select
     id as location_id,
     coalesce(name, concat('Unknown location ', cast(id as string))) as location_name,
     country,
     latitude,
     longitude,
     timezone,
     is_mobile,
     is_monitor,
     sensors,
     instruments
from {{ ref('stg_locations') }}