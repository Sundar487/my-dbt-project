with 

car_supplier_wise_itinerary_fare_details_table as (
    select 
        car_supplier_wise_itinerary_fare_detail_id as cswifd_car_supplier_wise_itinerary_fare_detail_id,
        car_itinerary_id as cswifd_car_itinerary_id,
        booking_master_id as cswifd_booking_master_id,
        supplier_account_id as car_supplier_account_id,
        consumer_account_id as car_consumer_account_id,
        portal_markup, 
        currency_code, 
        selected_currency_code, 
        settlement_exchange_rate,
        converted_exchange_rate,
        converted_currency_code,
        settlement_currency,
        supplier_markup,
        payment_mode,
        base_fare, 
        total_fare,
        onfly_markup,
        addon_charge,
        payment_charge,
        booking_fee,
        amt_pay_now,
        amt_pay_desk
    from {{ source('clarity_qa', 'car_supplier_wise_itinerary_fare_details') }}
)

select * from car_supplier_wise_itinerary_fare_details_table
