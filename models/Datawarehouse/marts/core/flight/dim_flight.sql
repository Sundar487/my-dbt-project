{{
  config(
    materialized = 'table'
    )
}}




with 

base_flight_itinerary as (

    select * from {{ ref('stg_flight_itinerary') }}

),


base_flight_journey as (

    select * from {{ ref('int_flight_journey_flight_segment') }}

),

stg_booking_master as (

    SELECT 
        bm_booking_master_id as f_booking_master_id,
        booking_type as f_booking_type
    FROM {{ ref('stg_booking_master') }}

),


final1 as (
    select * from base_flight_itinerary fi 
    inner join base_flight_journey fj
    on fi.fi_flight_itinerary_id = fj.fj_flight_itinerary_id
    inner join stg_booking_master bm 
    on bm.f_booking_master_id = fi.fi_booking_master_id 
    where f_booking_type = 1
),

final as (

    select * from final1
--   where fi_booking_master_id = 41396
    order by fi_flight_itinerary_id, fj_flight_journey_id
)

select * from final








