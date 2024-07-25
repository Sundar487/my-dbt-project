{{
  config(
    materialized = 'table',
    )
}}


with fct as (

    select  
--        {{ dbt_utils.generate_surrogate_key(['aswfd_activity_details_id', 'activity_date']) }} as activity_key, 
        bm_booking_master_id,
        bm_booking_req_id,
        activity_supplier_wise_fare_details_id as fct_activity_supplier_wise_fare_details_id,
        aswfd_activity_details_id as fct_activity_details_id,
        bm_portal_id,
        booking_status as activity_booking_status,
        payment_status as activity_payment_status,
        
        booking_source as car_booking_source,
        activity_parent_supplier_account_id, 
        activity_net_total as fct_activity_net_total,

        ROUND(
        COALESCE(activity_net_total::numeric, 0) *
        COALESCE(exchange_rate_equivalent_value::numeric, 1), 2) AS fct_activity_net_total_cad,

        exchange_rate_to_currency,
        exchange_rate_equivalent_value,

        activity_date as fct_activity_date,
        created_at as activity_booking_date,
        updated_at as activity_updated_at

    from {{ ref('stg_booking_master') }} bm

    inner join {{ ref('int_activity_parent_supplier') }} aps 
    on aps.int_aswfd_booking_master_id = bm.bm_booking_master_id

    inner join {{ ref('dim_activity') }} a
    on a.ad_booking_master_id = bm.bm_booking_master_id

    inner join {{ ref('dim_activity_fare_details') }} afd 
    on afd.aswfd_booking_master_id	= bm.bm_booking_master_id

    left join {{ ref('stg_currency_exchange_rate') }} cer
    on afd.activity_converted_currency = cer.exchange_rate_from_currency
--    where bm.bm_month = cer.cer_month and bm.bm_year = cer.cer_year
)

select * from fct






