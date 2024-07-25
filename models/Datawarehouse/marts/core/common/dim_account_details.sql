{{
  config(
    materialized = 'table'
    )
}}


with 

base_account_details_table as (
    select * from {{ ref('stg_account_details') }}

)

select * from base_account_details_table

