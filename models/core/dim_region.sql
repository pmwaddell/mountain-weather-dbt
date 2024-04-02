{{
    config(
        materialized='table'
    )
}}


select
    *
from {{ ref('dim_region_seed') }}