with 

insurance_itinerary_fare_details_table as (
    select 
        insurance_itinerary_fare_detail_id,
        booking_master_id as iifd_booking_master_id,
        insurance_itinerary_id as iifd_insurance_itinerary_id,
        converted_exchange_rate as iifd_converted_exchange_rate,
        base_fare as iifd_base_fare,
        tax as iifd_tax,
        total_fare as iifd_total_fare,
        payment_charge as iifd_payment_charge

    from {{ source('clarity_qa', 'insurance_itinerary_fare_details') }}
)

select * from insurance_itinerary_fare_details_table



