{{
    config(
        materialized='table'
    )
}}


select
    *
from 
    {{ ref("forecast_predictions") }}
where
    {{ dbt_date.date_part("month", "local_time_of_forecast") }} = 5 and
    {{ dbt_date.date_part("year", "local_time_of_forecast") }} = 2024
