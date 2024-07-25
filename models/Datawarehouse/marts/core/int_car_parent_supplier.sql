{{
  config(
    materialized = 'table',
    )
}}

with 

car_supplier_wise_itinerary_fare_details_table as (
    select *
    from {{ source('clarity_qa', 'car_supplier_wise_itinerary_fare_details') }}
),

temp_cswifd as (
    select 
        min(car_supplier_wise_itinerary_fare_detail_id) as  
        car_supplier_wise_itinerary_fare_detail_id
    from car_supplier_wise_itinerary_fare_details_table
    GROUP BY booking_master_id
),

inter as (
    select 
        cswifd.car_supplier_wise_itinerary_fare_detail_id as cswifd_car_supplier_wise_itinerary_fare_detail_id,
        cswifd.car_itinerary_id,
        cswifd.booking_master_id as cswifd_booking_master_id,
        cswifd.supplier_account_id,
        cswifd.consumer_account_id
    from temp_cswifd temp
    inner join car_supplier_wise_itinerary_fare_details_table cswifd
    on temp.car_supplier_wise_itinerary_fare_detail_id = cswifd.car_supplier_wise_itinerary_fare_detail_id
),


final as (
    select 
        cswifd_car_supplier_wise_itinerary_fare_detail_id as int_cswifd_car_supplier_wise_itinerary_fare_detail_id, 
        cswifd_booking_master_id as int_cswifd_booking_master_id,
        car_itinerary_id as int_cswifd_car_itinerary_id, 
        supplier_account_id as car_parent_supplier_account_id
    from inter
)

select * from final
order by int_cswifd_booking_master_id




