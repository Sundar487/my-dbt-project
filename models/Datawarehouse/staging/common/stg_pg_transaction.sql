with 

pg_transaction_table as (
    select 
        gateway_id as pg_gateway_id, 
        pg_transaction_id, 
        account_id as pg_account_id, 
        portal_id as pg_portal_id, 
        order_id as pg_order_id, 
        order_type, 
        pg_request_id as invoice_number, 
        payment_amount, 
        payment_fee, 
        transaction_amount, 
        currency, 
        pg_txn_reference as TRANSACTION_ID, 
        bank_txn_reference, 
        pg_source,
        transaction_status, 
        txn_initiated_date, 
        txn_completed_date

    from {{ source('clarity_qa', 'pg_transaction_details') }}
),

booking_master_table as (
    select 
        booking_master_id as pg_booking_master_id, 
        booking_req_id as pg_booking_req_id,
        payment_mode,
        payment_details
    from {{ source('clarity_qa', 'booking_master') }} 
),


temp_table as (
    SELECT MAX(pg_transaction_id) AS max_passenger_id, pg.pg_order_id as temp_pg_order_id
    FROM pg_transaction_table pg
    GROUP BY pg.pg_order_id
),

inter_pg as (
    select * from temp_table 
    inner join pg_transaction_table pg
    on temp_table.max_passenger_id = pg.pg_transaction_id
),

final as (
    select * from booking_master_table bm inner join inter_pg pg
    on bm.pg_booking_master_id = pg.pg_order_id
)


select * from final