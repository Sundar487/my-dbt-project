with 


base_flight_itinerary as (

    select * from {{ ref('stg_flight_itinerary') }}

),


base_flight_journey as (

    select * from {{ ref('int_flight_journey_flight_segment') }}

),


stg_booking_master as (

    SELECT 
        bm_booking_master_id as fh_booking_master_id,
        bm_booking_req_id as fh_booking_req_id,
        booking_type as fh_booking_type
    FROM {{ ref('stg_booking_master') }}

),


final1 as (
    select * from base_flight_itinerary fi 
    inner join base_flight_journey fj
    on fi.fi_flight_itinerary_id = fj.fj_flight_itinerary_id
    inner join stg_booking_master bm 
    on bm.fh_booking_master_id = fi.fi_booking_master_id
    where fh_booking_type = 4
    
)



select * from final1
order by fi_booking_master_id, fi_flight_itinerary_id, fj_flight_journey_id	


