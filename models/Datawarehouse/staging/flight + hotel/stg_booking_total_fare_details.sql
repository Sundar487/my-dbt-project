with 

booking_total_fare_details_table as (


    SELECT
        booking_total_fare_details_id,
        booking_master_id AS btfd_booking_master_id,
        ROUND((base_fare::numeric * converted_exchange_rate::numeric), 2) AS combined_base_fare,
        ROUND((tax::numeric * converted_exchange_rate::numeric), 2) AS combined_tax,
        ROUND((total_fare::numeric * converted_exchange_rate::numeric), 2) AS combined_total_fare,
        ROUND(
        COALESCE(onfly_markup::numeric, 0) *
        COALESCE(converted_exchange_rate::numeric, 0), 2) AS combined_agent_markup,           

        ROUND(
            (
                (
                    COALESCE(addon_charge::numeric, 0) + COALESCE(portal_surcharge::numeric, 0) +
                    COALESCE(payment_charge::numeric, 0) + COALESCE(booking_fee::numeric, 0)
                ) * converted_exchange_rate::numeric
            ), 2) AS combined_other_fees,


         ROUND((total_fare::numeric * converted_exchange_rate::numeric), 2) + 
        ROUND(
            (
                (
                    COALESCE(addon_charge::numeric, 0) + COALESCE(portal_surcharge::numeric, 0) +
                    COALESCE(payment_charge::numeric, 0) + COALESCE(booking_fee::numeric, 0)
                ) * converted_exchange_rate::numeric
            ), 2) + 
        ROUND(
        COALESCE(onfly_markup::numeric, 0) *
        COALESCE(converted_exchange_rate::numeric, 0), 2)  
        AS combined_net_total

    FROM
        {{ source('clarity_qa', 'booking_total_fare_details') }}
)



select * from booking_total_fare_details_table
-- where btfd_booking_master_id in  ('41139', '41287', '41373')