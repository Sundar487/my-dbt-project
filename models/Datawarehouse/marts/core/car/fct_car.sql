{{
  config(
    materialized = 'table',
    )
}}

with fct1 as (
    select

--      {{ dbt_utils.generate_surrogate_key(['ci_pickup_date']) }} as date_key,
        bm_booking_master_id,
        bm_booking_req_id,
        bm_portal_id,
        booking_status as car_booking_status,
        payment_status as car_payment_status,

        booking_source as car_booking_source, 
        car_parent_supplier_account_id,

        car_net_total as fct_car_net_total,
        car_converted_currency as fct_car_converted_currency,

        ROUND(
        COALESCE(car_net_total::numeric, 0) *
        COALESCE(exchange_rate_equivalent_value::numeric, 1) / 
        COALESCE(rental_days::numeric, 0), 2) AS average_car_rental_daily_rate,

        ROUND(
        COALESCE(car_net_total::numeric, 0) *
        COALESCE(exchange_rate_equivalent_value::numeric, 1), 2) AS fct_car_net_total_cad,

        exchange_rate_to_currency,
        exchange_rate_equivalent_value,

        created_at as car_booking_date,
        ci_pickup_date as pickup_date,
        updated_at as car_updated_at

    from {{ ref('stg_booking_master') }} bm
    inner join {{ ref('dim_car_transaction') }} ct
    on bm.bm_booking_master_id  = ct.cswifd_booking_master_id
    inner join {{ ref('int_car_parent_supplier') }} cp 
    on bm.bm_booking_master_id = cp.int_cswifd_booking_master_id
    inner join {{ ref('dim_car_details') }} cd 
    on bm.bm_booking_master_id = cd.ci_booking_master_id
    left join {{ ref('stg_currency_exchange_rate') }} cer
    on ct.car_converted_currency = cer.exchange_rate_from_currency
--    where bm.bm_month = cer.cer_month and bm.bm_year = cer.cer_year
)

select * from fct1
-- where bm_booking_master_id = '41210'
order by bm_booking_master_id




