with 

insurance_supplier_wise_booking_total_table as (
    select 

        insurance_supplier_wise_booking_total_id,
        supplier_account_id as iswbt_supplier_account_id,
        booking_master_id as iswifd_booking_master_id,
        consumer_account_id as iswbt_consumer_account_id,
        is_own_content as iswbt_is_own_content,
        insurance_itinerary_id as iswbt_insurance_itinerary_id,
        converted_exchange_rate,
        converted_currency,
        settlement_currency,
        settlement_exchange_rate,
        base_fare, 
        tax, 
        total_fare ,
        payment_charge, 
        onfly_markup,
        onfly_discount,
        portal_surcharge,
        supplier_markup,
        supplier_discount,
        addon_charge,
        portal_markup,
        booking_fee

    from {{ source('clarity_qa', 'insurance_supplier_wise_booking_total') }}
)

select * from insurance_supplier_wise_booking_total_table
