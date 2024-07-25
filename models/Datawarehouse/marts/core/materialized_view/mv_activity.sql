{{
  config(
    materialized = 'materialized_view',
  )
}}


select * from {{ ref('activity_query') }}