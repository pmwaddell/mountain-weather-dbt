{{
    config(
        materialized='table'
    )
}}


select
    {{ dbt_utils.generate_surrogate_key(['mtn_name']) }} time_zone_key,
    time_zone,
    utc_diff
from {{ ref('stg_sun_and_time_zone') }}
group by mtn_name, time_zone, utc_diff