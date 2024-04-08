with source as (
    select * from {{ source('forecast_staging', 'forecasts') }}
),

renamed as (
    select
        mtn_name,
        elevation,
        cast(time_of_scrape as datetime) as time_of_scrape,
        cast(local_time_issued as datetime) as local_time_issued,
        forecast_status,
        cast(local_time_of_forecast as datetime) as local_time_of_forecast,
        cast(date_trunc(cast(local_time_of_forecast as datetime), day) as date) as local_date_of_forecast,
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
        round(exp(-0.000119 * elevation), 3) as air_pressure
    from source
)

select * from renamed
