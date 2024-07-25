with 

passenger_table as (
    select * from {{ source('clarity_qa', 'flight_passenger') }}
),


booking_master_table as (
    select 
        booking_master_id as fp_booking_master_id, 
        booking_req_id as fp_booking_req_id
    from {{ source('clarity_qa', 'booking_master') }} 
),


inter as (
    select * from booking_master_table bm inner join passenger_table pt
    on bm.fp_booking_master_id = pt.booking_master_id
),

final as (
    select 
        fp_booking_master_id, 
        fp_booking_req_id, 
        flight_passenger_id as passenger_id,	
        salutation, 
        first_name,	
        last_name, 
        gender, 
        dob, 
        email_address,
        ffp, 
        contact_no_country_code, 
        contact_no, 
        pax_type, 
        created_at,	
        updated_at  
    from inter 
)

select * from final