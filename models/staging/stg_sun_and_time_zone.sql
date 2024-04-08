with source as (
    select * from {{ source('sun_staging', 'sun_and_time_zone_data') }}
),

renamed as (
    select
        mtn_name,
        time_zone,
        utc_diff,
        date_trunc(cast(scrape_date as date), day) as scrape_date,
        cast(sunrise_time as datetime) as sunrise_time,
        cast(sunset_time as datetime) as sunset_time
    from source
)

select * from renamed
