{{ config(full_refresh = false) }}


with 

dim_account_details as (
    select * from {{ ref('dim_account_details') }}
),


dim_car_details as (
    select * from {{ ref('dim_flight') }}
),

dim_car_transaction as (
    select * from {{ ref('dim_flight_fare') }}
),

dim_portal_details as (
    select * from {{ ref('dim_portal_details') }}
),

fct_car as (
    select * from {{ ref('fct_flight') }}
),

final as (
    select * from dim_date d
    left join fct_flight ff
    on d.date  = ff.fct_travel_date

    left join dim_portal_details pd
    on ff.bm_portal_id  = pd.pd_portal_id

    left join dim_flight f
    on f.fj_flight_journey_id = ff.fct_fj_flight_journey_id

    left join dim_account_details ad
    on ff.swf_flight_parent_supplier_account_id = ad.ad_account_id

    left join dim_flight_fare dff
    on dff.swbt_booking_master_id = ff.bm_booking_master_id
    

)

select * from final
order by bm_booking_master_id, fct_fi_flight_itinerary_id, fct_fj_flight_journey_id

