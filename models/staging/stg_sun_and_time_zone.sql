with source as (
    select * from {{ source('sun_staging', 'sun_and_time_zone_data') }}
),

renamed as (
    select
        mtn_name,
        time_zone,
        utc_diff,
        date_trunc(cast(scrape_date as timestamp), day) as scrape_date,
        sunrise_time,
        sunset_time
    from source
)

select * from renamed
