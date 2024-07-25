{{
  config(
    materialized = 'materialized_view',
  )
}}


select * from {{ ref('insurance_query') }}

