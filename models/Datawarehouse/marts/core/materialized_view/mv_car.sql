{{
  config(
    materialized = 'materialized_view',
  )
}}


select * from {{ ref('car_query') }}

