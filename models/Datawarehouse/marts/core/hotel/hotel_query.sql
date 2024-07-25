{{ config(full_refresh = false) }}

{{
  config(
    materialized = 'incremental',
    unique_key = 'hotel_room_details_id',
    on_schema_change = 'append_new_columns'
    )
}}


with 

dim_account_details as (
    select * from {{ ref('dim_account_details') }}
),


dim_hotel as (
    select * from {{ ref('dim_hotel') }}
),

dim_hotel_fare as (
    select * from {{ ref('dim_hotel_fare') }}
),

dim_portal_details as (
    select * from {{ ref('dim_portal_details') }}
),

fct_car as (
    select * from {{ ref('fct_hotel') }}
),

final as (
    select * from dim_date d
    left join fct_hotel fh
    on d.date  = fh.fct_hotel_check_in_date

    left join dim_portal_details pd
    on fh.bm_portal_id  = pd.pd_portal_id

    left join dim_hotel h
    on h.hotel_room_details_id = fh.fct_hotel_room_details_id

    left join dim_account_details ad
    on fh.swhifd_parent_hotel_supplier_account_id = ad.ad_account_id

    left join dim_hotel_fare dhf
    on dhf.swhbt_booking_master_id = fh.bm_booking_master_id
    
    {% if is_incremental() %}
    where hotel_booking_date > (select max(hotel_booking_date) from {{this}})
    {% endif %}
)

select * from final
order by bm_booking_master_id, hotel_room_details_id




