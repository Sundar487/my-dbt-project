{{
  config(
    materialized = 'table',
    )
}}


with fct_insurance as (
    select  
        bm_booking_master_id,
        bm_booking_req_id,
        bm_portal_id,

        booking_status,
        payment_status,

        insurance_parent_supplier_account_id,
        insurance_date as fct_insurance_date,
        insurance_net_total as fct_insurance_net_total,

        ROUND(
        COALESCE(insurance_net_total::numeric, 0) *
        COALESCE(exchange_rate_equivalent_value::numeric, 1), 2) AS fct_insurance_net_total_cad,

        exchange_rate_to_currency,
        exchange_rate_equivalent_value,

        created_at as insurance_booking_date,
        updated_at as insurance_updated_at

        
    from {{ ref('stg_booking_master') }} bm
    inner join {{ ref('stg_status_details') }} sd
    on bm.booking_status  = sd.status_id 
    inner join {{ ref('int_insurance_parent_supplier_id') }} ips 
    on ips.int_iswifd_booking_master_id = bm.bm_booking_master_id
    inner join {{ ref('dim_insurance_fare_details') }} ifd 
    on ifd.iswifd_booking_master_id = bm.bm_booking_master_id
    inner join {{ ref('dim_insurance') }} i
    on i.ii_booking_master_id = bm.bm_booking_master_id

    left join {{ ref('stg_currency_exchange_rate') }} cer
    on ifd.converted_currency = cer.exchange_rate_from_currency

)

select * from fct_insurance         

