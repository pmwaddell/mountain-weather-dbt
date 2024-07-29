{{
    config(
        materialized='table'
    )
}}

with fact_forecasts_actual_june_2024_am as (
    select
        *
    from {{ ref("fact_forecasts_actual") }}
    where
        forecast_time_name = 'am' and
        {{ dbt_date.date_part("month", "local_time_of_forecast") }} = 6 and
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
    round(avg(chill), 2) as avg_chill,
    round(stddev(chill), 2) as std_dev_chill,
    round(avg(wind_speed), 2) as avg_wind_speed
from fact_forecasts_actual_june_2024_am
group by
    mtn_name,
    region_group_key,
    mtn_range,
    subrange,
    latitude,
    longitude,
    geo_dimension,
    elevation,
    elevation_feature