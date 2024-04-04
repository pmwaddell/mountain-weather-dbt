with source as (
    select * from {{ source('forecast_staging', 'forecasts') }}
),

renamed as (
    select
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
    from source
)

select * from renamed
