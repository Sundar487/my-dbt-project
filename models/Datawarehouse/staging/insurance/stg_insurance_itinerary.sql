with 

insurance_itinerary_table as (
    select 
        insurance_itinerary_id as ii_insurance_itinerary_id,
        booking_master_id as ii_booking_master_id,
        provider_name as insurance_booking_source,
        plan_name as ins_plan_name,
        departure_airport as ins_departure_airport,
        arrival_airport as ins_arrival_airport,
        departure_date as ins_departure_date,
        arrival_date as ins_arrival_date,
        policy_number,
        plan_code

    from {{ source('clarity_qa', 'insurance_itinerary') }}
)

select * from insurance_itinerary_table