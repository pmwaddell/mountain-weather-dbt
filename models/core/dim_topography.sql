{{
    config(
        materialized='table'
    )
}}


select
    {{ dbt_utils.generate_surrogate_key(['mtn_name']) }} as topography_key,
    *
from {{ ref('dim_topography_seed') }}