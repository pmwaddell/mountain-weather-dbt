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
        time_of_scrape,
        local_time_issued,
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
        forecast_phrase, 
        wind_speed,
        snow,
        rain,
        max_temp,
        min_temp,
        chill,
        freezing_level,
        cloud_base,
        air_pressure
    from {{ ref("stg_forecasts") }}
),

-- Often, two "actual" rows will be present for a single forecast time
-- In this case, I retain the one with the later issued date (time_rank = 1)
-- For this table, DO NOT use fact_forecasts_actual because, in that table,
-- we had to drop the local_time_issued column, which we need here.
forecasts_one_actual as (
    select * from forecasts
    where forecast_status = 'forecast' or time_rank = 1
),

forecasts_for_join as (
    select * from forecasts_one_actual
    where forecast_status = 'actual'
)

select
    f.fact_forecasts_key,
    f.mtn_name,
    g.region_group_key,
    g.mtn_range,
    g.subrange,
    g.latitude,
    g.longitude,
    g.geo_dimension,
    f.elevation,
    feat.elevation_feature,
    f.time_of_scrape,
    f.local_time_issued,
    f.local_time_of_forecast,
    extract(day from (f.local_time_of_forecast - f.local_time_issued)) * 24 +
    extract(hour from (f.local_time_of_forecast - f.local_time_issued)) as time_diff_hours,
    f.forecast_time_name,
    f.forecast_status,
    f.forecast_phrase,
    f.wind_speed,
    f2.wind_speed AS actual_wind_speed,
    f.wind_speed - f2.wind_speed as wind_speed_diff,
    abs(f.wind_speed - f2.wind_speed) as abs_wind_speed_diff,
    f.snow,
    f2.snow AS actual_snow,
    round(f.snow - f2.snow, 1) as snow_diff,
    round(abs(f.snow - f2.snow), 1) as abs_snow_diff,
    f.rain,
    f2.rain AS actual_rain,
    round(f.rain - f2.rain, 1) as rain_diff,
    round(abs(f.rain - f2.rain), 1) as abs_rain_diff,
    f.max_temp,
    f2.max_temp AS actual_max_temp,
    f.max_temp - f2.max_temp as max_temp_diff,
    abs(f.max_temp - f2.max_temp) as abs_max_temp_diff,
    f.min_temp,
    f2.min_temp AS actual_min_temp,
    f.min_temp - f2.min_temp as min_temp_diff,
    abs(f.min_temp - f2.min_temp) as abs_min_temp_diff,
    f.chill,
    f2.chill AS actual_chill,
    f.chill - f2.chill as chill_diff,
    abs(f.chill - f2.chill) as abs_chill_diff
from forecasts_one_actual as f

left join dim_geography as g on
    f.geography_key = g.geography_key
left join dim_mf_features as feat on
    f.mf_features_key = feat.mf_features_key
inner join forecasts_for_join as f2 on
    f.local_time_of_forecast = f2.local_time_of_forecast and
    f.mtn_name = f2.mtn_name and
    f.elevation = f2.elevation


-- For debugging:
-- where f.mtn_name = 'mont_blanc' and f.elevation = 4000
-- and f.local_time_of_forecast = '2024-04-09T07:00:00'
-- order by f.forecast_status ASC, f.local_time_issued DESC