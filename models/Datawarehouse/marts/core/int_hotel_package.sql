{{
  config(
    materialized = 'table',
    )
}}


with 

hotel_itinerary as (
    select * from {{ ref('stg_hotel_itinerary') }} 
),

hotel_room_details as (
    select * from {{ ref('stg_hotel_room_details') }} 
),

stg_booking_master as (

    SELECT 
        bm_booking_master_id as fh_booking_master_id,
        bm_booking_req_id as fh_booking_req_id,
        booking_type as fh_booking_type

    FROM {{ ref('stg_booking_master') }}

),

final as (
    select 
        hi_booking_master_id,	
        hi_booking_req_id,
        hotel_reservation_code_pnr,
        confirmation_no,

        hotel_room_details_id,
        hi_hotel_itinerary_id,
        hotel_name,
        hotel_city,	
        hotel_supplier,
        hotel_rating,	
        no_of_rooms, 
        
        type_of_stay,	
        number_of_nights,
        check_in_date,	
        check_out_date,	
  
        room_name,	
        board_name,	
        
        no_of_adult,	
        no_of_child
     
    from hotel_room_details hrd 
    inner join hotel_itinerary hi 
    on hrd.hrd_hotel_itinerary_id = hi.hi_hotel_itinerary_id
    inner join stg_booking_master bm 
    on bm.fh_booking_master_id = hi.hi_booking_master_id
    where fh_booking_type = 4
)

select * from final
order by hi_booking_master_id, hi_hotel_itinerary_id, hotel_room_details_id	
-- where hi_booking_master_id = 40738

