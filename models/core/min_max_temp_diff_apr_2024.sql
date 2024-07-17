{{
    config(
        materialized='table'
    )
}}

with dim_geography as (
    select
        geography_key,
        region_group_key,
        mtn_range,
        subrange,
        latitude,
        longitude,
        concat(latitude , ",", longitude) AS geo_dimension
    from {{ ref("dim_geography") }}
),

dim_mf_features as (
    select
        mf_features_key,
        elevation_feature
    from {{ ref("dim_mf_features") }}
),

forecasts as (
    select
        {{ 
            dbt_utils.generate_surrogate_key(
                ['mtn_name', 'elevation', 'local_time_issued', 'local_time_of_forecast']
            )
        }} as fact_forecasts_key,
        {{ dbt_utils.generate_surrogate_key(['mtn_name']) }} as geography_key,
        mtn_name,
        elevation,
        {{ dbt_utils.generate_surrogate_key(['mtn_name', 'elevation']) }} as mf_features_key,
        forecast_status,
        local_time_of_forecast,
        RANK() OVER (
            PARTITION BY
                mtn_name,
                elevation, 
                local_time_of_forecast
            ORDER BY local_time_issued DESC
        ) AS time_rank,
        forecast_time_name,
        wind_speed,
        max_temp,
        min_temp,
        chill
    from {{ ref("stg_forecasts") }}
        where 
        forecast_status = 'actual' and
        {{ dbt_date.date_part("month", "local_time_of_forecast") }} = 4 and
        {{ dbt_date.date_part("year", "local_time_of_forecast") }} = 2024
),

-- Often, two "actual" rows will be present for a single forecast time
-- In this case, I retain the one with the later issued date (time_rank = 1)
top_time_ranked as (
    select * from forecasts
    where time_rank = 1
)

select
    f.mtn_name,
    g.region_group_key,
    g.mtn_range,
    g.subrange,
    g.latitude,
    abs(g.latitude) as abs_latitude,
    g.longitude,
    abs(g.longitude) as abs_longitude,
    g.geo_dimension,
    f.elevation,
    feat.elevation_feature,
    f.forecast_time_name,
    round(avg(f.max_temp) - avg(f.min_temp), 2) as min_max_temp_diff,
    round(avg(f.wind_speed), 2) as avg_wind_speed
from top_time_ranked as f

left join dim_geography 
    as g on f.geography_key = g.geography_key
left join dim_mf_features
    as feat on f.mf_features_key = feat.mf_features_key

group by
    f.mtn_name, 
    g.region_group_key,
    g.mtn_range,
    g.subrange,
    g.latitude,
    g.longitude,
    g.geo_dimension,
    f.elevation,
    feat.elevation_feature,
    f.forecast_time_name