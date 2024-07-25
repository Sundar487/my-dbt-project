{{
  config(
    materialized = 'table',
    )
}}

with 

int_supplier_wise_flight_table as (

    SELECT * FROM {{ ref('stg_supplier_wise_itinerary_fare_details') }}
     
),



temp_swf as (
    select 
        min(flight_supplier_wise_itinerary_fare_detail_id) as  
        temp_supplier_wise_itinerary_fare_detail_id
    from int_supplier_wise_flight_table 
    GROUP BY swifd_booking_master_id
),


inter as (
    select * from int_supplier_wise_flight_table swf
    inner join temp_swf temp
    on swf.flight_supplier_wise_itinerary_fare_detail_id  = temp.temp_supplier_wise_itinerary_fare_detail_id  
),



final as (
    select  
        swifd_booking_master_id as swf_min_booking_master_id,
        swifd_supplier_account_id as swf_flight_parent_supplier_account_id   
    from inter
)

select * from final
order by swf_min_booking_master_id

-- select count(*) from final  33447