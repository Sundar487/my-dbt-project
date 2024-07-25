{{
  config(
    materialized = 'table',
    )
}}

with 

int_supplier_wise_hotel_table as (

    SELECT * FROM {{ ref('stg_supplier_wise_hotel_booking_total') }}
     
),

stg_booking_master as (

    SELECT 

        bm_booking_master_id as hf_booking_master_id,
        bm_booking_req_id as hf_booking_req_id,
        booking_type as hf_booking_type

    FROM {{ ref('stg_booking_master') }}

),


temp_swh as (
    select 
        max(supplier_wise_hotel_booking_total_id) as  
        temp_supplier_wise_hotel_booking_total_id
    from int_supplier_wise_hotel_table 
    GROUP BY swhbt_booking_master_id
),



final1 as (
    select * from int_supplier_wise_hotel_table swh
    inner join temp_swh temp
    on swh.supplier_wise_hotel_booking_total_id  = temp.temp_supplier_wise_hotel_booking_total_id  
),



final as (
    select 
        supplier_wise_hotel_booking_total_id,
        swhbt_booking_master_id,
        swhbt_supplier_account_id,
        swhbt_consumer_account_id,

        settlement_currency as hotel_settlement_currency,
        converted_currency as hotel_converted_currency,

        settlement_exchange_rate as hotel_settlement_exchange_rate,
        converted_exchange_rate as hotel_converted_exchange_rate,

        ROUND((total_fare * converted_exchange_rate)::numeric, 2) - 
        ROUND(COALESCE(supplier_markup::numeric * converted_exchange_rate::numeric, 0) + 
        COALESCE(portal_markup::numeric * converted_exchange_rate::numeric, 0), 2) as HOTEL_BASE_FARE,
        
        ROUND(
        COALESCE(onfly_markup::numeric, 0) * 
        COALESCE(converted_exchange_rate::numeric, 0), 2) AS HOTEL_AGENT_MARKUP,

        ROUND((ssr_fare * converted_exchange_rate)::numeric, 2) AS HOTEL_SSR_FARE,
        ROUND((tax * converted_exchange_rate)::numeric, 2) AS HOTEL_TAX, 
        ROUND(total_fare::numeric, 2) AS HOTEL_INVENTORY_AMOUNT,
        ROUND((total_fare * converted_exchange_rate)::numeric, 2) AS HOTEL_TOTAL_FARE,

        ROUND(COALESCE(supplier_markup::numeric * converted_exchange_rate::numeric, 0) + 
        COALESCE(portal_markup::numeric * converted_exchange_rate::numeric, 0), 2) AS hotel_markup,

        ROUND(COALESCE(promo_discount::numeric * converted_exchange_rate::numeric, 0), 2) as discount,

        ROUND(COALESCE(total_fare::numeric * converted_exchange_rate::numeric, 0)  + 
        COALESCE(booking_fee::numeric * converted_exchange_rate::numeric, 0)  +
        COALESCE(onfly_markup::numeric * converted_exchange_rate::numeric, 0)  -
        COALESCE(promo_discount::numeric * converted_exchange_rate::numeric, 0), 2) as HOTEL_NET_TOTAL

    from final1 f 
    inner join stg_booking_master bm
    on f.swhbt_booking_master_id = bm.hf_booking_master_id
    where hf_booking_type = 2

)

select * from final
-- where swhifd_booking_master_id = '40629'


