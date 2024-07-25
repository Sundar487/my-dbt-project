with 

flight_journey_table as (

    select 
        flight_journey_id as fj_flight_journey_id,
        flight_itinerary_id as fj_flight_itinerary_id,
        departure_airport AS fj_departure_airport,
        arrival_airport AS fj_arrival_airport,
        departure_date_time AS Travel_Date
          
    from {{ source('clarity_qa', 'flight_journey') }}

)


-- select * from flight_journey_table
-- order by fi_booking_master_id desc
-- arrival_airport AS DESTINATION
select * from flight_journey_table



