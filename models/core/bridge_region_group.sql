{{
    config(
        materialized='table'
    )
}}


select
    *
from {{ ref('bridge_region_group_seed') }}