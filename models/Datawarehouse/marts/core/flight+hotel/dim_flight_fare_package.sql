{{
  config(
    materialized = 'table'
    )
}}

with 

dim_supplier_wise_flight_table as (

    SELECT * FROM {{ ref('stg_supplier_wise_booking_total') }}
     
),

stg_booking_master as (

    SELECT 
        bm_booking_master_id as ff_booking_master_id,
        bm_booking_req_id as ff_booking_req_id,
        booking_type as ff_booking_type

    FROM {{ ref('stg_booking_master') }}

),


temp_swf as (
    select 
        max(supplier_wise_booking_total_id) as  
        temp_supplier_wise_booking_total_id
    from dim_supplier_wise_flight_table 
    GROUP BY swbt_booking_master_id
),



final as (
    select * from dim_supplier_wise_flight_table swf
    inner join temp_swf temp
    on swf.supplier_wise_booking_total_id  = temp.temp_supplier_wise_booking_total_id  
),

supplier_wise_booking_total as (

    SELECT
        supplier_wise_booking_total_id,
        swbt_booking_master_id,
        flight_supplier_account_id,
        flight_consumer_account_id,
        settlement_exchange_rate as flight_settlement_exchange_rate,
        converted_exchange_rate as flight_converted_exchange_rate,
        converted_currency as flight_converted_currency,
        settlement_currency as flight_settlement_currency,
        payment_mode as swbt_payment_mode,

        ROUND((base_fare::numeric * converted_exchange_rate::numeric), 2) AS FLIGHT_BASE_FARE, 
        ROUND((tax::numeric * converted_exchange_rate::numeric), 2) AS FLIGHT_TAX, 
        ROUND((total_fare::numeric * converted_exchange_rate::numeric), 2) AS FLIGHT_TOTAL_FARE,

        ROUND(
        COALESCE(onfly_markup::numeric, 0) *
        COALESCE(converted_exchange_rate::numeric, 0), 2) AS FLIGHT_AGENT_MARKUP,

        ROUND(
            (
                (
                    COALESCE(supplier_discount::numeric, 0) + COALESCE(onfly_discount::numeric, 0) +
                    COALESCE(portal_discount::numeric, 0) + COALESCE(promo_discount::numeric, 0)
                ) * converted_exchange_rate::numeric
            ), 2) AS FLIGHT_DISCOUNT,
        ROUND((COALESCE(portal_markup::numeric, 0) * converted_exchange_rate::numeric), 2) AS FLIGHT_MARK_UP, 
        ROUND(
            (
                (
                    COALESCE(addon_charge::numeric, 0) + COALESCE(portal_surcharge::numeric, 0) +
                    COALESCE(payment_charge::numeric, 0) + COALESCE(booking_fee::numeric, 0)
                ) * converted_exchange_rate::numeric
            ), 2) AS FLIGHT_OTHER_FEES,
        ROUND(
            (
                (
                    COALESCE(supplier_markup::numeric, 0) + COALESCE(addon_charge::numeric, 0) +
                    COALESCE(portal_surcharge::numeric, 0) + COALESCE(payment_charge::numeric, 0) +
                    COALESCE(booking_fee::numeric, 0)
                ) * converted_exchange_rate::numeric
            ), 2) AS FLIGHT_NET_MARGIN,


        ROUND((total_fare::numeric * converted_exchange_rate::numeric), 2) + 
        ROUND(
            (
                (
                    COALESCE(addon_charge::numeric, 0) + COALESCE(payment_charge::numeric, 0) + COALESCE(booking_fee::numeric, 0)
                ) * converted_exchange_rate::numeric
            ), 2) + 
        ROUND(
        COALESCE(onfly_markup::numeric, 0) *
        COALESCE(converted_exchange_rate::numeric, 0), 2)  
        AS FLIGHT_NET_TOTAL
     
    FROM final f
    inner join stg_booking_master bm 
    on bm.ff_booking_master_id = f.swbt_booking_master_id
    where ff_booking_type = 4
)

select * from supplier_wise_booking_total