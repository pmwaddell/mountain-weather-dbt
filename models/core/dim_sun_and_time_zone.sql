{{
    config(
        materialized='table'
    )
}}


select
    {{ dbt_utils.generate_surrogate_key(['mtn_name']) }} sun_and_time_zone_key,
    *,
    sunset_time - sunrise_time as total_daylight
from {{ ref('stg_sun_and_time_zone') }}