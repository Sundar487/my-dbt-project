with 

activity_supplier_wise_fare_details_table as (
    select
        activity_supplier_wise_fare_details_id,
        booking_master_id as aswfd_booking_master_id,
        activity_details_id as aswfd_activity_details_id,
        supplier_account_id as activity_supplier_account_id,
        consumer_account_id as activity_consumer_account_id,
        payment_mode,
        base_fare,
        tax,
        total_fare,
        supplier_discount,
        onfly_markup,
        onfly_discount,
        converted_currency,
        settlement_currency,
        payment_charge,
        converted_exchange_rate,
        booking_status

    from {{ source('clarity_qa', 'activity_supplier_wise_fare_details') }}
)

select * from activity_supplier_wise_fare_details_table


