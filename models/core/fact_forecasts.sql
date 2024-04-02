{{
    config(
        materialized='table'
    )
}}


select
    {{ 
        dbt_utils.generate_surrogate_key(
            ['mtn_name', 'elevation', 'local_time_issued', 'local_time_of_forecast']
        )
    }} fact_forecasts_key,
    {{ dbt_utils.generate_surrogate_key(['mtn_name']) }} geography_key,
    mtn_name,
    elevation,
    elev_feature,
    time_of_scrape,
    local_time_issued,
    forecast_status,
    local_time_of_forecast,
    forecast_time_name,
    forecast_phrase, 
    wind_speed,
    snow,
    rain,
    max_temp,
    min_temp,
    chill,
    freezing_level,
    cloud_base
from {{ ref("stg_forecasts") }}