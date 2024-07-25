{{
  config(
    materialized = 'table'
    )
}}

with 
passenger_table as (

    select * from {{ ref('stg_passenger') }} 
)

select * from passenger_table 