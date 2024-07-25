with 

flight_itinerary_table as (

    select 
        flight_itinerary_id as fi_flight_itinerary_id,
        booking_master_id as fi_booking_master_id,
        gds AS FLIGHT_BOOKING_SOURCE,
        itinerary_id,
        fare_type,
       (CASE  WHEN is_international = 'Y' THEN 'INTERNATIONAL' WHEN is_international = 'N' THEN 'DOMESTIC' END) AS DOMESTIC_INTERNATIONAL 
    from {{ source('clarity_qa', 'flight_itinerary') }}

)


-- select * from flight_itinerary_table
-- order by fi_booking_master_id desc
select * from flight_itinerary_table