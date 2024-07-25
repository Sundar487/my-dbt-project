with 

supplier_wise_booking_total as (

    SELECT
        supplier_wise_booking_total_id,
        booking_master_id AS swbt_booking_master_id,
        supplier_account_id as flight_supplier_account_id,
        consumer_account_id as flight_consumer_account_id,
        payment_mode,
        base_fare,
        tax,
        total_fare,
        onfly_discount,
        promo_discount,
        supplier_discount,
        portal_discount,
        portal_markup,
        addon_charge,
        portal_surcharge,
        supplier_markup,
        payment_charge,
        booking_fee,
        onfly_markup,
        settlement_exchange_rate,
        converted_exchange_rate,
        converted_currency,
        settlement_currency
     
    FROM
        {{ source('clarity_qa', 'supplier_wise_booking_total') }}
)


select * from supplier_wise_booking_total

-- where swbt_booking_master_id in ('41139', '41287', '41373')