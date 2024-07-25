{{
  config(
    materialized = 'table',
  )
}}


with 

dim_account_details as (
    select * from {{ ref('dim_account_details') }}
),


dim_car_details as (
    select * from {{ ref('dim_car_details') }}
),

dim_car_transaction as (
    select * from {{ ref('dim_car_transaction') }}
),

dim_portal_details as (
    select * from {{ ref('dim_portal_details') }}
),

fct_car as (
    select * from {{ ref('fct_car') }}
),


final as (
    select * from dim_date d
    left join fct_car fc 
    on d.date  = fc.pickup_date

    left join dim_portal_details pd
    on fc.bm_portal_id  = pd.pd_portal_id

    left join dim_car_details cd
    on cd.ci_booking_master_id = fc.bm_booking_master_id

    left join dim_account_details ad
    on fc.car_parent_supplier_account_id = ad.ad_account_id

    left join dim_car_transaction ct
    on ct.cswifd_booking_master_id = fc.bm_booking_master_id

)

select * from final
