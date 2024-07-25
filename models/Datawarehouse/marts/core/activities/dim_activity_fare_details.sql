{{
  config(
    materialized = 'table',
    )
}}

with 

activity_supplier_wise_fare_details_table as (

    SELECT * FROM {{ ref('stg_activity_supplier_wise_fare_details') }}
     
),


temp_aswfd as (
    select 
        max(activity_supplier_wise_fare_details_id) as  
        temp_activity_supplier_wise_fare_details_id
    from activity_supplier_wise_fare_details_table 
    GROUP BY aswfd_booking_master_id
),

stg_booking_master as (

    SELECT 
        bm_booking_master_id as afd_booking_master_id,
        booking_type as afd_booking_type
    FROM {{ ref('stg_booking_master') }}

),

final1 as (
    select * from activity_supplier_wise_fare_details_table aswfd
    inner join temp_aswfd temp
    on aswfd.activity_supplier_wise_fare_details_id  = temp.temp_activity_supplier_wise_fare_details_id  
),



final as (
    select 
        activity_supplier_wise_fare_details_id,
        aswfd_booking_master_id,
        aswfd_activity_details_id,
--        activity_supplier_account_id,
        activity_consumer_account_id,
        settlement_currency as activity_settlement_currency,
        converted_currency as activity_converted_currency,
        payment_mode as activity_payment_mode,
        base_fare as activity_base_fare,
        tax as activity_tax,
        total_fare as activity_total_fare,

        ROUND(
        (COALESCE(total_fare::numeric, 0) +
        COALESCE(payment_charge::numeric, 0) *
        COALESCE(converted_exchange_rate::numeric, 0))
        , 2) AS activity_net_total,

        supplier_discount as activity_supplier_discount,
        onfly_markup as activity_onfly_markup,
        onfly_discount as activity_onfly_discount,
        payment_charge as activity_payment_charge
        
    from final1 f inner join stg_booking_master bm
    on f.aswfd_booking_master_id = bm.afd_booking_master_id
    where afd_booking_type = 6 and afd_booking_master_id not in (
    select  aswfd_booking_master_id from activity_supplier_wise_fare_details_table group by aswfd_booking_master_id
    having count(*) > 1)  -- 18
)

select * from final 
-- where aswfd_booking_master_id in (41305, 41285, 36668, 34130, 41112)




