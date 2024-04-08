{{
    config(
        materialized='table'
    )
}}


select
    {{ dbt_utils.generate_surrogate_key(['mtn_name']) }} as mountaineering_key,
    *
from {{ ref('dim_mountaineering_seed') }}