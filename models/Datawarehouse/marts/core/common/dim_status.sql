with dim_status as (
    select  
        bm_booking_master_id,
        bm_booking_req_id,
        status_name as booking_status,

        CASE
        WHEN  ticket_status = '201' THEN 'Initiated'
        WHEN  ticket_status = '202' THEN 'Ticketed' 
        WHEN  ticket_status = '203' THEN 'Failed'
        WHEN  ticket_status = '204' THEN 'Cancelled'
        WHEN  ticket_status = '205' THEN 'Partially Ticketed' 
        END AS ticket_status,

        CASE  
        WHEN  payment_status = '301' THEN 'Initiated' 
        WHEN  payment_status = '302' THEN 'Success'
        WHEN  payment_status = '303' THEN 'failed' 
        WHEN  payment_status = '304' THEN 'GDS Paid'
        END AS payment_status

       

    from {{ ref('stg_booking_master') }} bm
    inner join {{ ref('stg_status_details') }} sd
    on bm.booking_status  = sd.status_id
)



select * from dim_status