{{
    config(
        materialized='table'
    )
}}


select
    {{ dbt_utils.generate_surrogate_key(['mtn_name']) }} as geography_key,
    *
from {{ ref('dim_geography_seed') }}