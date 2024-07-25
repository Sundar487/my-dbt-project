{{
  config(
    materialized = 'table',
    )
}}


with 

stg_booking_master as (

    SELECT 
        bm_booking_master_id as i_booking_master_id,
        booking_type as i_booking_type
    FROM {{ ref('stg_booking_master') }}

),

insurance_itinerary as (
    select
        ii_insurance_itinerary_id,
        ii_booking_master_id,
        insurance_booking_source,
        ins_plan_name,
        ins_departure_date as insurance_date,
        policy_number,
        plan_code
    from {{ ref('stg_insurance_itinerary') }}
),

final as (
    select * from stg_booking_master bm inner join insurance_itinerary ii 
    on bm.i_booking_master_id = ii.ii_booking_master_id
    where i_booking_type = 3
)


select * from final
