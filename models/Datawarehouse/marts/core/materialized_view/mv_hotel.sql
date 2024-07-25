{{
  config(
    materialized = 'materialized_view',
  )
}}


select * from {{ ref('hotel_query') }}

