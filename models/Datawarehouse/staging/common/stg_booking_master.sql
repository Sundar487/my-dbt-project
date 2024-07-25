with 

booking_master_table as (
select 
    booking_master_id as bm_booking_master_id, 
    booking_req_id as bm_booking_req_id,
    account_id as bm_account_id,
    portal_id as bm_portal_id,
    booking_source,
    booking_res_id,
    booking_type,

    (CASE 
    WHEN trip_type = '1' THEN 'Oneway' 
    WHEN trip_type = '2' THEN 'Roundtrip' 
    WHEN trip_type = '3' THEN 'Multicity' 
    ELSE '0'
    END) AS TRAVEL_TYPE, 

    pos_currency AS FLIGHT_INVENTORY_CURRENCY,
    
    payment_mode,
    booking_status,
    ticket_status,
    payment_status,
    pax_split_up,
    total_pax_count,   
    retry_booking_count,
    created_at, 
    updated_at,
    EXTRACT(MONTH FROM created_at)as bm_month,
    EXTRACT(YEAR FROM created_at) as bm_year
    
from {{ source('clarity_qa', 'booking_master') }}
)

select * from booking_master_table
order by bm_booking_master_id



