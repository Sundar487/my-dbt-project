{{
  config(
    materialized = 'table',
    )
}}


with fct as (
    select  
        bm_booking_master_id,
        bm_booking_req_id,
        bm_portal_id,
--        swhifd_hotel_itinerary_id as hotel_itinerary_id,
        hotel_room_details_id as fct_hotel_room_details_id,
        booking_status as hotel_booking_status,
        payment_status as hotel_payment_status,
        booking_source as hotel_booking_source,

        swhifd_parent_hotel_supplier_account_id,

        hotel_check_in_date as fct_hotel_check_in_date,
        hotel_net_total as fct_hotel_net_total,

        ROUND(
        COALESCE(hotel_net_total::numeric, 0) *
        COALESCE(exchange_rate_equivalent_value::numeric, 1), 2) AS fct_hotel_net_total_cad,

        exchange_rate_to_currency,
        exchange_rate_equivalent_value,

        created_at as hotel_booking_date,
        updated_at as hotel_updated_at

    from {{ ref('stg_booking_master') }} bm
    inner join {{ ref('stg_status_details') }} sd
    on bm.booking_status  = sd.status_id 
    inner join {{ ref('int_hotel_parent_supplier') }} s
    on  bm.bm_booking_master_id = s.swhifd_min_booking_master_id 
    inner join {{ ref('dim_hotel_fare') }} swh
    on bm.bm_booking_master_id = swh.swhbt_booking_master_id
    inner join {{ ref('dim_hotel') }} dh
    on bm.bm_booking_master_id = dh.hi_booking_master_id

    left join {{ ref('stg_currency_exchange_rate') }} cer
    on swh.hotel_converted_currency = cer.exchange_rate_from_currency
--    where bm.bm_month = cer.cer_month and bm.bm_year = cer.cer_year and bm.booking_type = 1
    where bm.booking_type = 2
)

select * from fct
-- where swhifd_min_booking_master_id= '40629'

-- swhifd_booking_master_id,  swhbt_consumer_account_id,


