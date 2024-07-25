{{
  config(
    materialized = 'table',
    )
}}


with 

dim_account_details as (
    select * from {{ ref('dim_account_details') }}
),


dim_insurance as (
    select * from {{ ref('dim_insurance') }}
),

dim_insurance_fare_details as (
    select * from {{ ref('dim_insurance_fare_details') }}
),

dim_portal_details as (
    select * from {{ ref('dim_portal_details') }}
),

fct_insurance as (
    select * from {{ ref('fct_insurance') }}
),

final as (
    select * from dim_date d
    left join fct_insurance fi
    on d.date  = fi.fct_insurance_date

    left join dim_portal_details pd
    on fi.bm_portal_id  = pd.pd_portal_id

    left join dim_insurance i
    on i.ii_booking_master_id = fi.bm_booking_master_id

    left join dim_account_details ad
    on fi.insurance_parent_supplier_account_id = ad.ad_account_id

    left join dim_insurance_fare_details ifd
--    on afd.aswfd_booking_master_id = fa.bm_booking_master_id
    on ifd.iswifd_booking_master_id = fi.bm_booking_master_id

)

select * from final

