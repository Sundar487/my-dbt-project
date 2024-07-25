{{
  config(
    materialized = 'table'
    )
}}

with 

base_portal_details_table as (
    select
        pd_portal_id,
        pd_account_id,
        parent_portal_id, 
        portal_name,
        portal_url,
        business_type,
        pd_agency_name
    from {{ ref('stg_portal_details') }}
)

select * from base_portal_details_table