{{
  config(
    materialized = 'table',
    )
}}

with 

stg_booking_master as (

    SELECT 
        bm_booking_master_id as a_booking_master_id,
        booking_type as a_booking_type
    FROM {{ ref('stg_booking_master') }}

),


activity_details_table as (

    SELECT * FROM {{ ref('stg_activity_details') }}
     
),

final as (
  select * from stg_booking_master bm  inner join activity_details_table ad
  on bm.a_booking_master_id  = ad.ad_booking_master_id
  where a_booking_type = 6 and a_booking_master_id not in (
  select  ad_booking_master_id from activity_details_table group by ad_booking_master_id
  having count(*) > 1)  -- 18
)

select * from final
