with 

supplier_wise_hotel_booking_total_table as (

    SELECT
        supplier_wise_hotel_booking_total_id,
        booking_master_id as swhbt_booking_master_id,
        supplier_account_id as swhbt_supplier_account_id,
        is_own_content,
        consumer_account_id as swhbt_consumer_account_id,
        settlement_exchange_rate,
        converted_exchange_rate,
        converted_currency,
        settlement_currency,
        total_fare,
        onfly_markup,
        supplier_markup,
        portal_markup,
        promo_discount,
        ssr_fare,
        tax,
        booking_fee
    FROM
        {{ source('clarity_qa', 'supplier_wise_hotel_booking_total') }}
)



select * from supplier_wise_hotel_booking_total_table

-- where swhbt_booking_master_id = '40629'
