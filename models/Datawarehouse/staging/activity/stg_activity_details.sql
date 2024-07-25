with 

activity_details_table as (
    select
        activity_details_id,
        booking_master_id as ad_booking_master_id,
        activity_id,
        confirmation_no,
        fare_type,
        activity_name,
        activity_rating,
        activity_date,
        pickup_point,
        drop_point,
        ROUND(CAST(duration AS NUMERIC) / 60.0, 2) AS duration_in_hours
--        booking_status,
--        payment_status
    from {{ source('clarity_qa', 'activity_details') }}
)

select * from activity_details_table