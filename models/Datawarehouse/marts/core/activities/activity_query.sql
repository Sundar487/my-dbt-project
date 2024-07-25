{{
  config(
    materialized = 'table',
    )
}}



with 

dim_account_details as (
    select * from {{ ref('dim_account_details') }}
),


dim_activity as (
    select * from {{ ref('dim_activity') }}
),

dim_activity_fare_details as (
    select * from {{ ref('dim_activity_fare_details') }}
),

dim_portal_details as (
    select * from {{ ref('dim_portal_details') }}
),

fct_activities as (
    select * from {{ ref('fct_activity') }}
),

final as (
    select * from dim_date d
    left join fct_activities fa
    on d.date  = fa.fct_activity_date

    left join dim_portal_details pd
    on fa.bm_portal_id  = pd.pd_portal_id

    left join dim_activity a
    on a.activity_details_id = fa.fct_activity_details_id

    left join dim_account_details ad
    on fa.activity_parent_supplier_account_id = ad.ad_account_id

    left join dim_activity_fare_details afd
--    on afd.aswfd_booking_master_id = fa.bm_booking_master_id
    on a.activity_details_id = afd.aswfd_activity_details_id

)

select * from final


