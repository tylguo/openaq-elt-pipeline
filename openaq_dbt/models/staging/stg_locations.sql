{{ config(
    materialized = 'view'
) }}

select
    *
from {{ source('openaq_raw', 'locations') }}
