{{
  config(
    materialized = 'materialized_view',
  )
}}


select * from {{ ref('flight_query') }}