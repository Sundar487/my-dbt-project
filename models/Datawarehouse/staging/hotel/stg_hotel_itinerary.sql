with 

hotel_itinerary_table as (

    select 
        booking_master_id as hi_booking_master_id,
        bm_booking_req_id as hi_booking_req_id,
        hotel_name AS HOTEL_NAME,
        hotel_itinerary_id AS hi_hotel_itinerary_id,
        itinerary_id, 
        pnr AS HOTEL_RESERVATION_CODE_PNR, 
        hi.star_rating AS HOTEL_RATING, 
        hi.destination_city AS HOTEL_CITY, 
        hi.gds AS HOTEL_SUPPLIER, 
        (CASE WHEN hi.type_of_stay = 'F' THEN 'FULL' WHEN hi.type_of_stay = 'P' THEN 'PARTIAL' END) AS TYPE_OF_STAY,
        extract(day from hi.check_out::timestamp - hi.check_in::timestamp) AS NUMBER_OF_NIGHTS, 
        hi.room_count AS NO_OF_ROOMS, 
        hi.check_in AS CHECK_IN_DATE, 
        hi.check_out AS CHECK_OUT_DATE
    from {{ source('clarity_qa', 'hotel_itinerary') }} hi 
    inner join {{ ref('stg_booking_master') }} bm 
    on hi.booking_master_id = bm.bm_booking_master_id
  
)

select * from hotel_itinerary_table