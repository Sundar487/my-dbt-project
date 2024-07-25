with 

supplier_wise_itinerary_fare_details_table as (

    SELECT  
        supplier_wise_itinerary_fare_detail_id as flight_supplier_wise_itinerary_fare_detail_id,
        booking_master_id AS swifd_booking_master_id,
        flight_itinerary_id,
        contract_remarks,
        supplier_account_id as swifd_supplier_account_id,
        consumer_account_id as swifd_consumer_account_id
    FROM
        {{ source('clarity_qa', 'supplier_wise_itinerary_fare_details') }}
)

select * from supplier_wise_itinerary_fare_details_table