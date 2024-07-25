{{
  config(
    materialized = 'table',
    )
}}


with 

insurance_supplier_wise_booking_total_table as (

    SELECT * FROM {{ ref('stg_insurance_supplier_wise_booking_total') }}
     
),


temp_iswbt as (
    select 
        max(insurance_supplier_wise_booking_total_id) as  
        temp_insurance_supplier_wise_booking_total_id
    from insurance_supplier_wise_booking_total_table 
    GROUP BY iswifd_booking_master_id
),

stg_booking_master as (

    SELECT 
        bm_booking_master_id as ifd_booking_master_id,
        booking_type as ifd_booking_type
    FROM {{ ref('stg_booking_master') }}

),



final1 as (
    select * from insurance_supplier_wise_booking_total_table iswbt
    inner join temp_iswbt temp
    on iswbt.insurance_supplier_wise_booking_total_id  = temp.temp_insurance_supplier_wise_booking_total_id  
),


insurance_fare_details_table as (
    select 
        insurance_supplier_wise_booking_total_id,
        iswifd_booking_master_id,
--        iswbt_supplier_account_id          
        iswbt_consumer_account_id,
        iswbt_is_own_content,
        iswbt_insurance_itinerary_id,
        converted_exchange_rate,
        converted_currency,
        settlement_currency,
        settlement_exchange_rate,
        
        ROUND((base_fare::numeric * converted_exchange_rate::numeric), 2) AS INSURANCE_BASE_FARE, 
        ROUND((tax::numeric * converted_exchange_rate::numeric), 2) AS INSURANCE_TAX, 
        ROUND((total_fare::numeric * converted_exchange_rate::numeric), 2) AS INSURANCE_TOTAL_FARE,

        ROUND((total_fare::numeric * converted_exchange_rate::numeric), 2) + 
        ROUND(
            (
                (
                    COALESCE(addon_charge::numeric, 0) + COALESCE(portal_surcharge::numeric, 0) +
                    COALESCE(payment_charge::numeric, 0) + COALESCE(booking_fee::numeric, 0)
                ) * converted_exchange_rate::numeric
            ), 2) + 
        ROUND(
        COALESCE(onfly_markup::numeric, 0) *
        COALESCE(converted_exchange_rate::numeric, 0), 2)  
        AS INSURANCE_NET_TOTAL

    from final1 f inner join stg_booking_master bm
    on f.iswifd_booking_master_id = bm.ifd_booking_master_id
    where ifd_booking_type  = 3

)

select * from insurance_fare_details_table

