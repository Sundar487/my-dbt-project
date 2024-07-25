{{
  config(
    materialized = 'table',
    )
}}


with fct as (
    select  
        bm_booking_master_id,
        bm_booking_req_id,

        fi_flight_itinerary_id,
        fj_flight_journey_id,
        flight_booking_source,
        booking_status as flight_booking_status,
        ticket_status as flight_ticket_status,
        payment_status as flight_payment_status,
        flight_hotel_parent_supplier_account_id,

        hi_hotel_itinerary_id,
        hotel_room_details_id,
        hotel_name,
        hotel_reservation_code_pnr,
        
        HOTEL_SUPPLIER,

        FLIGHT_NET_TOTAL,
        hotel_net_total,
        FLIGHT_NET_TOTAL + hotel_net_total as flight_hotel_net_total

    from {{ ref('stg_booking_master') }} bm
    inner join {{ ref('stg_status_details') }} sd
    on bm.booking_status  = sd.status_id 
    
    inner join {{ ref('int_flight_hotel_parent_supplier') }} fps 
    on bm.bm_booking_master_id = fps.swf_min_booking_master_id

    inner join {{ ref('dim_flight_hotel_package') }} hi
    on hi.hi_booking_master_id = bm.bm_booking_master_id

    inner join {{ ref('dim_hotel_fare_package') }} swh
    on hi.hi_booking_master_id  = swh.swhbt_booking_master_id

    inner join {{ ref('dim_flight_fare_package') }} swbt
    on swbt.swbt_booking_master_id = swh.swhbt_booking_master_id
     
)

select * from fct
-- where bm_booking_master_id in ('41287', '40902', '1641', '41311')
order by bm_booking_master_id, fi_flight_itinerary_id, fj_flight_journey_id, hi_hotel_itinerary_id, hotel_room_details_id

