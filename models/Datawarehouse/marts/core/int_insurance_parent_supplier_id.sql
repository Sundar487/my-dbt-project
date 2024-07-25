with 

insurance_supplier_wise_itinerary_fare_details_table as (

    SELECT * FROM {{ ref('stg_insurance_supplier_wise_itinerary_fare_details') }}
     
),


temp_iswbt as (
    select 
        min(insurance_supplier_wise_itinerary_fare_detail_id) as  
        temp_insurance_supplier_wise_itinerary_fare_detail_id
    from insurance_supplier_wise_itinerary_fare_details_table 
    GROUP BY iswifd_booking_master_id
),



final1 as (
    select * from insurance_supplier_wise_itinerary_fare_details_table iswifd
    inner join temp_iswbt temp
    on iswifd.insurance_supplier_wise_itinerary_fare_detail_id  = temp.temp_insurance_supplier_wise_itinerary_fare_detail_id  
),


insurance_fare_details_table as (
    select 

        insurance_supplier_wise_itinerary_fare_detail_id as int_insurance_supplier_wise_itinerary_fare_detail_id,
        iswifd_booking_master_id as int_iswifd_booking_master_id,
        iswifd_supplier_account_id as insurance_parent_supplier_account_id 

    from final1
)

select * from insurance_fare_details_table