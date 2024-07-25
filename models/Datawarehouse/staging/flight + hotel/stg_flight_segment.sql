with 

flight_segment_table as (

    select 
        flight_segment_id as fs_flight_segment_id,
        flight_journey_id as fs_flight_journey_id,
        departure_airport AS fs_departure_airport,
        arrival_airport AS fs_arrival_airport,
        cabin_class,
        fare_basis_code,
        booking_class,
        airline_code,
        airline_name,
        departure_date_time,
        operating_airline AS Flown_Carrier,
        marketing_airline 
    from {{ source('clarity_qa', 'flight_segment') }}

)


select * from flight_segment_table
-- select * from flight_segment_table