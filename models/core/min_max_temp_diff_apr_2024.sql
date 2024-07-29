{{
    config(
        materialized='table'
    )
}}

with fact_forecasts_actual_may_2024 as (
    select
        *
    from {{ ref("fact_forecasts_actual") }}
    where
        {{ dbt_date.date_part("month", "local_time_of_forecast") }} = 4 and
        {{ dbt_date.date_part("year", "local_time_of_forecast") }} = 2024
)

select
    mtn_name,
    region_group_key,
    mtn_range,
    subrange,
    latitude,
    abs(latitude) as abs_latitude,
    longitude,
    abs(longitude) as abs_longitude,
    geo_dimension,
    elevation,
    elevation_feature,
    forecast_time_name,
    round(avg(max_temp) - avg(min_temp), 2) as min_max_temp_diff,
    round(avg(wind_speed), 2) as avg_wind_speed
from fact_forecasts_actual_may_2024
group by
    mtn_name, 
    region_group_key,
    mtn_range,
    subrange,
    latitude,
    longitude,
    geo_dimension,
    elevation,
    elevation_feature,
    forecast_time_name