with 

int_flight_parent_supplier as (

    SELECT * FROM {{ ref('int_flight_parent_supplier') }}
     
),


int_hotel_parent_supplier as (

    select * from {{ ref('int_hotel_parent_supplier') }}

),

stg_booking_master as (

    select 
        bm_booking_master_id as int_fh_booking_master_id,
        booking_type as int_fh_booking_type
    from {{ ref('stg_booking_master') }}
),


final as (

    select
        swf_min_booking_master_id,	
        swf_flight_parent_supplier_account_id as flight_hotel_parent_supplier_account_id,	
        swhifd_min_booking_master_id,	
        swhifd_parent_hotel_supplier_account_id,
        int_fh_booking_master_id,	
        int_fh_booking_type 
    from int_flight_parent_supplier f 
    inner join int_hotel_parent_supplier h 
    on f.swf_min_booking_master_id = h.swhifd_min_booking_master_id
    inner join stg_booking_master bm
    on bm.int_fh_booking_master_id = h.swhifd_min_booking_master_id
    where int_fh_booking_type = 4

)

select * from final 







































