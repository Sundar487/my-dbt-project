{{
  config(
    materialized = 'table'
    )
}}


with 

base_currency_exchange_rate as (  
    
    select * from {{ ref('stg_currency_exchange_rate') }}

)

select * from base_currency_exchange_rate