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
        longitude
    from {{ ref("dim_geography") }}
),

dim_sun_and_time_zone as (
    select
        sun_and_time_zone_key,
        time_zone,
        utc_diff,
        scrape_date,
        sunrise_time,
        sunset_time,
        total_daylight
    from {{ ref("dim_sun_and_time_zone") }}
),

dim_topography as (
    select
        topography_key,
        elevation as wiki_elevation,
        elevation_rank,
        prominence,
        prominence_rank,
        isolation,
        isolation_rank
    from {{ ref("dim_topography") }}
),

dim_mf_features as (
    select
        mf_features_key,
        elevation_feature
    from {{ ref("dim_mf_features") }}
),

dim_mountaineering as (
    select
        mountaineering_key,
        first_ascent,
        ascents,
        fatalities,
        fatalities_per_summit_rate
    from {{ ref("dim_mountaineering") }}
),

forecasts as (
    select
        {{ 
            dbt_utils.generate_surrogate_key(
                ['mtn_name', 'elevation', 'local_time_issued', 'local_time_of_forecast']
            )
        }} fact_forecasts_key,
        {{ dbt_utils.generate_surrogate_key(['mtn_name']) }} geography_key,
        {{ dbt_utils.generate_surrogate_key(['mtn_name']) }} sun_and_time_zone_key,
        {{ dbt_utils.generate_surrogate_key(['mtn_name']) }} topography_key,
        {{ dbt_utils.generate_surrogate_key(['mtn_name']) }} mountaineering_key,
        mtn_name,
        elevation,
        {{ dbt_utils.generate_surrogate_key(['mtn_name', 'elevation']) }} mf_features_key,
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
)

select

    f.mtn_name,
    g.region_group_key,
    g.mtn_range,
    g.subrange,
    g.latitude,
    g.longitude,
    s.time_zone,
    s.utc_diff,
    s.scrape_date,
    s.sunrise_time,
    s.sunset_time,
    s.total_daylight,
    t.wiki_elevation,
    t.elevation_rank,
    t.prominence,
    t.prominence_rank,
    t.isolation,
    t.isolation_rank,
    m.first_ascent,
    m.ascents,
    m.fatalities,
    m.fatalities_per_summit_rate,
    f.elevation,
    feat.elevation_feature,
    f.time_of_scrape,
    f.local_time_issued,
    f.forecast_status,
    f.local_time_of_forecast,
    f.forecast_time_name,
    f.forecast_phrase, 
    f.wind_speed,
    f.snow,
    f.rain,
    f.max_temp,
    f.min_temp,
    f.chill,
    f.freezing_level,
    f.cloud_base

from forecasts as f

left join dim_geography 
    as g on f.geography_key = g.geography_key
left join dim_sun_and_time_zone 
    as s on f.sun_and_time_zone_key = s.sun_and_time_zone_key 
    and date_trunc(cast(f.local_time_of_forecast as timestamp), day) = date_trunc(cast(s.scrape_date as timestamp), day)
left join dim_topography 
    as t on f.topography_key = t.topography_key
left join dim_mountaineering 
    as m on f.mountaineering_key = m.mountaineering_key
left join dim_mf_features 
    as feat on f.mf_features_key = feat.mf_features_key