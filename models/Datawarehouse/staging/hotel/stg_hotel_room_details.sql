with 

hotel_room_details_table as (

    select 
        hotel_room_details_id,
        booking_master_id as hrd_booking_master_id,
        hotel_itinerary_id as hrd_hotel_itinerary_id,
        room_name,
        board_name,
        confirmation_no,
        no_of_adult,
        no_of_child
    from {{ source('clarity_qa', 'hotel_room_details') }}

)

select * from hotel_room_details_table