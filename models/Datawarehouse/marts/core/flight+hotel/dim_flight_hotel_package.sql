with 

int_flight_package as (

    select * from {{ ref('int_flight_package') }}

),

int_hotel_package as (

    select * from {{ ref('int_hotel_package') }}

),


fi_cte as (
select fi_booking_master_id as int_fi_booking_master_id,
	  fi_flight_itinerary_id as int_fi_flight_itinerary_id, 
	  fj_flight_journey_id as int_fj_flight_journey_id,
	  ROW_NUMBER() OVER (Partition BY fi_booking_master_id order by fj_flight_journey_id ) AS fi_row_num 
from int_flight_package
),

final1 as (
select * from int_flight_package f inner join fi_cte c 
on f.fj_flight_journey_id = c.int_fj_flight_journey_id
),


hi_cte as (
select hi_booking_master_id as int_hi_booking_master_id,
	  hi_hotel_itinerary_id as int_hi_hotelitinerary_id, 
	  hotel_room_details_id as int_hotel_room_details_id,
	  ROW_NUMBER() OVER (Partition BY hi_booking_master_id order by hotel_room_details_id ) AS hi_row_num 
from int_hotel_package
),

final2 as (
select * from int_hotel_package h inner join hi_cte c 
on h.hotel_room_details_id = c.int_hotel_room_details_id
),

-- select * from final2 where hi_booking_master_id = 37378

final as (
SELECT *
FROM final2 h
left JOIN final1 f ON f.fi_booking_master_id = h.hi_booking_master_id  
and f.fi_row_num = h.hi_row_num
)

select * from final
