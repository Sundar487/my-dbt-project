with 

insurance_supplier_wise_itinerary_fare_details_table as (
    select 
        insurance_supplier_wise_itinerary_fare_detail_id,
        booking_master_id as iswifd_booking_master_id,
        insurance_itinerary_id as iswifd_insurance_itinerary_id,
        supplier_account_id as iswifd_supplier_account_id,
        consumer_account_id as iswifd_consumer_account_id,
        base_fare as iswifd_base_fare,
        tax as iswifd_tax,
        total_fare as iswifd_total_fare,
        payment_charge as iswifd_payment_charge

    from {{ source('clarity_qa', 'insurance_supplier_wise_itinerary_fare_details') }}
)

select * from insurance_supplier_wise_itinerary_fare_details_table


