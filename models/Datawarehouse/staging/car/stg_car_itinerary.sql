with 

car_itinerary_table as (
    select *
    from {{ source('clarity_qa', 'car_itinerary') }}
),


booking_master_table as (
    select 
        booking_master_id as ci_booking_master_id, 
        booking_req_id as ci_booking_req_id
    from {{ source('clarity_qa', 'booking_master') }}
    
),


inter as (
    select * from booking_master_table bm inner join car_itinerary_table ci
    on bm.ci_booking_master_id = ci.booking_master_id
),

final as (
    select 
        car_itinerary_id as ci_car_itinerary_id, 
        ci_booking_master_id,
        ci_booking_req_id, 
        sup_email, 
        sup_phone,
        provider_name, 
        pcc, 
        supplier_name, 
        gds_pnr, 
        mileage, 
        confirmation_no,
        passengers, 
        transmission, 
        car_make, 
        car_type, 
        fuel_type, 
        rental_days, 
        pickup_location, 
        dropoff_location, 	
        pickup_date, 
        dropoff_date 
    --    booking_status, 
    --    payment_status
    from inter
)

select * from final
