{{
  config(
    materialized = 'table',
  )
}}

with 

car_supplier_wise_itinerary_fare_details_table as (
    select * from {{ ref('stg_car_supplier_wise_itinerary_fare_details') }}
),

temp_cswifd as (
    select 
        max(cswifd_car_supplier_wise_itinerary_fare_detail_id) as  
        temp_car_supplier_wise_itinerary_fare_detail_id
    from car_supplier_wise_itinerary_fare_details_table
    GROUP BY cswifd_booking_master_id
),

inter as (
    select *
    from temp_cswifd temp
    inner join car_supplier_wise_itinerary_fare_details_table cswifdt
    on temp.temp_car_supplier_wise_itinerary_fare_detail_id = cswifd_car_supplier_wise_itinerary_fare_detail_id
),

-- where cswifdt_booking_master_id in (20988, 41210)

dim_car_transaction_table as (
    select      
        cswifd_booking_master_id, 
        cswifd_car_supplier_wise_itinerary_fare_detail_id,
--        car_supplier_account_id,
        car_consumer_account_id,
        settlement_exchange_rate as car_settlement_exchange_rate,
        converted_exchange_rate as car_converted_exchange_rate,
        payment_mode as cswifd_payment_mode,

        currency_code as car_converted_currency,
        selected_currency_code as car_settlement_currency,

        ROUND(
        COALESCE(base_fare::numeric, 0) *
        COALESCE(converted_exchange_rate::numeric, 0)
        -
        COALESCE(supplier_markup::numeric, 0) *
        COALESCE(converted_exchange_rate::numeric, 0)
        , 2) AS car_base_fare,

        ROUND(
        COALESCE(supplier_markup::numeric, 0) *
        COALESCE(converted_exchange_rate::numeric, 0), 2) AS car_supplier_markup,

        ROUND(
        COALESCE(total_fare::numeric, 0) *
        COALESCE(converted_exchange_rate::numeric, 0), 2) AS car_total_fare,

        

        ROUND(
        COALESCE(onfly_markup::numeric, 0) *
        COALESCE(converted_exchange_rate::numeric, 0), 2) AS car_agent_markup,


        ROUND(
        COALESCE(booking_fee::numeric, 0) *
        COALESCE(converted_exchange_rate::numeric, 0), 2) AS car_booking_fee,

        ROUND(
        COALESCE(base_fare::numeric, 0) * COALESCE(converted_exchange_rate::numeric, 0) +
        COALESCE(onfly_markup::numeric, 0) *  COALESCE(converted_exchange_rate::numeric, 0) +
        COALESCE(booking_fee::numeric, 0) * COALESCE(converted_exchange_rate::numeric, 0) +
        COALESCE(payment_charge::numeric, 0)
        , 2) AS car_net_total,
        
        amt_pay_now as car_amt_pay_now,
        amt_pay_desk as car_amt_pay_desk
        
    from inter
)

select * from dim_car_transaction_table  

--    inner join {{ ref('stg_portal_details') }} pd 
--    on pd.pd_portal_id = bm.bm_portal_id

