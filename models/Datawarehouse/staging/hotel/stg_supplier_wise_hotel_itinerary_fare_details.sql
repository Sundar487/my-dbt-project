with 

supplier_wise_hotel_itinerary_fare_details_table as (

    SELECT
        supplier_wise_hotel_itinerary_fare_detail_id,
        booking_master_id as swhifd_booking_master_id,
        hotel_itinerary_id as swhifd_hotel_itinerary_id,
        supplier_account_id as swhifd_supplier_account_id,
        consumer_account_id as swhifd_consumer_account_id
    FROM
        {{ source('clarity_qa', 'supplier_wise_hotel_itinerary_fare_details') }}
)

select * from supplier_wise_hotel_itinerary_fare_details_table   




