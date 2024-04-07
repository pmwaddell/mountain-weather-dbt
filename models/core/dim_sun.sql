{{
    config(
        materialized='table'
    )
}}


select
    {{ dbt_utils.generate_surrogate_key(['mtn_name', 'scrape_date']) }} sun_key,
    mtn_name,
    scrape_date,
    sunrise_time,
    sunset_time,
    sunset_time - sunrise_time as total_daylight
from {{ ref('stg_sun_and_time_zone') }}