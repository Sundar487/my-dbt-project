{{
  config(
    materialized = 'table',
    )
}}

with 

int_supplier_wise_hotel_table as (

    SELECT * FROM {{ ref('stg_supplier_wise_hotel_itinerary_fare_details') }}
     
),



temp_swh as (
    select 
        min(supplier_wise_hotel_itinerary_fare_detail_id) as  
        temp_supplier_wise_hotel_itinerary_fare_detail_id
    from int_supplier_wise_hotel_table 
    GROUP BY swhifd_booking_master_id
),



final1 as (
    select * from int_supplier_wise_hotel_table swh
    inner join temp_swh temp
    on swh.supplier_wise_hotel_itinerary_fare_detail_id  = temp.temp_supplier_wise_hotel_itinerary_fare_detail_id  
),



final as (
    select 
        swhifd_booking_master_id as swhifd_min_booking_master_id,
        swhifd_supplier_account_id as swhifd_parent_hotel_supplier_account_id
    from final1
)

select * from final
-- where swhifd_booking_master_id = '40629'