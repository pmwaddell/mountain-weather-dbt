{{
    config(
        materialized='table'
    )
}}

-- In some cases, there will be two issued forecasts that occur for the same
-- "timeslot" (i.e. date AM, PM or night). This leads to two forecasts in the
-- upserted table being labeled "actual". In order to agreeably resolve this 
-- apparent problem, their average is taken to be representative of the "true" 
-- value that occured during that period.
select
    {{ 
        dbt_utils.generate_surrogate_key(
            ['mtn_name', 'elevation', 'local_time_of_forecast']
        )
    }} as fact_forecasts_actual_key,
    mtn_name,
    region_group_key,
    mtn_range,
    subrange,
    latitude,
    longitude,
    geo_dimension,
    time_zone,
    utc_diff,
    sunrise_time,
    sunset_time,
    total_daylight,
    wiki_peak_elevation,
    elevation_rank,
    prominence,
    prominence_rank,
    isolation,
    isolation_rank,
    first_ascent,
    ascents,
    fatalities,
    fatalities_per_summit_rate,
    elevation,
    elevation_feature,
    local_time_of_forecast,
    forecast_time_name,
    round(avg(wind_speed), 2) as wind_speed,
    round(avg(snow), 2) as snow,
    round(avg(rain), 2) as rain,
    round(avg(max_temp), 2) as max_temp,
    round(avg(min_temp), 2) as min_temp,
    round(avg(chill), 2) as chill,
    round(avg(freezing_level), 2) as freezing_level,
    round(avg(cloud_base), 2) as cloud_base,
    air_pressure,
    p_o2,
    in_death_zone
from {{ ref("fact_forecasts") }}
where forecast_status = 'actual'
group by
    mtn_name,
    region_group_key,
    mtn_range,
    subrange,
    latitude,
    longitude,
    geo_dimension,
    time_zone,
    utc_diff,
    sunrise_time,
    sunset_time,
    total_daylight,
    wiki_peak_elevation,
    elevation_rank,
    prominence,
    prominence_rank,
    isolation,
    isolation_rank,
    first_ascent,
    ascents,
    fatalities,
    fatalities_per_summit_rate,
    elevation,
    elevation_feature,
    local_time_of_forecast,
    forecast_time_name,
    air_pressure,
    p_o2,
    in_death_zone
