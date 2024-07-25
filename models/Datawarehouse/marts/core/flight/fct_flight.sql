{{
  config(
    materialized = 'table',
    )
}}


with fct as (
    select  
        bm_booking_master_id,
        bm_booking_req_id,
        bm_portal_id,

        fi_flight_itinerary_id as fct_fi_flight_itinerary_id,
        fj_flight_journey_id as fct_fj_flight_journey_id,
        booking_status as flight_booking_status,
        ticket_status as flight_ticket_status,
        payment_status as flight_payment_status,

        swf_flight_parent_supplier_account_id,
        flight_booking_source as fct_flight_booking_source,

        date(travel_date) as fct_travel_date,
        FLIGHT_NET_TOTAL as fct_flight_net_total,

        ROUND(
        COALESCE(FLIGHT_NET_TOTAL::numeric, 0) *
        COALESCE(exchange_rate_equivalent_value::numeric, 1), 2) AS fct_flight_net_total_cad,

        exchange_rate_to_currency,
        exchange_rate_equivalent_value,

        created_at as flight_booking_date,
        updated_at as flight_updated_at

    from {{ ref('stg_booking_master') }} bm
    inner join {{ ref('stg_status_details') }} sd
    on bm.booking_status  = sd.status_id 
    inner join {{ ref('dim_flight') }} df 
    on bm.bm_booking_master_id = df.fi_booking_master_id
    inner join {{ ref('dim_flight_fare') }} swbt
    on swbt.swbt_booking_master_id = bm.bm_booking_master_id
    inner join {{ ref('int_flight_parent_supplier') }} fps 
    on bm.bm_booking_master_id = fps.swf_min_booking_master_id
    left join {{ ref('stg_currency_exchange_rate') }} cer
    on swbt.flight_converted_currency = cer.exchange_rate_from_currency
--    where bm.bm_month = cer.cer_month and bm.bm_year = cer.cer_year and bm.booking_type = 1
    where bm.booking_type = 1
    order by fi_flight_itinerary_id, fj_flight_journey_id 
)

select * from fct
-- where bm_booking_master_id in ('41287', '40902', '1641', '41311')
order by bm_booking_master_id, fct_fi_flight_itinerary_id, fct_fj_flight_journey_id






