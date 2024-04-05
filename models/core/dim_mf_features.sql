{{
    config(
        materialized='table'
    )
}}


select
    {{ dbt_utils.generate_surrogate_key(['mtn_name', 'elevation']) }} mf_features_key,
    *
from {{ ref('dim_mf_features_seed') }}