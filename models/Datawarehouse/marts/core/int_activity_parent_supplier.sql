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
        min(activity_supplier_wise_fare_details_id) as  
        temp_activity_supplier_wise_fare_details_id
    from activity_supplier_wise_fare_details_table 
    GROUP BY aswfd_booking_master_id
),



final1 as (
    select * from activity_supplier_wise_fare_details_table aswfd
    inner join temp_aswfd temp
    on aswfd.activity_supplier_wise_fare_details_id  = temp.temp_activity_supplier_wise_fare_details_id  
),



final as (
    select 

        activity_supplier_wise_fare_details_id as int_activity_supplier_wise_fare_details_id,
        aswfd_booking_master_id as int_aswfd_booking_master_id,
        aswfd_activity_details_id as int_aswfd_activity_details_id,
        activity_supplier_account_id as activity_parent_supplier_account_id
--        activity_consumer_account_id as int_activity_consumer_account_id,       
    from final1
)

select * from final 
-- where aswfd_booking_master_id in (41305, 41285, 36668, 34130)