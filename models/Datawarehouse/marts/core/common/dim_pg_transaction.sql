{{
  config(
    materialized = 'table',
  )
}}

with base_pg_transaction as (
    select * from {{ ref('stg_pg_transaction') }}
),


base_payment_gateway_details as (
    select * from {{ ref('stg_payment_gateway_details') }}
),


final as (
    select 
      distinct pg_booking_master_id, 
      pg_booking_req_id,	
      order_type,
      invoice_number ,	
      payment_amount,	
      payment_fee,
      payment_mode, 
      transaction_amount,	
      currency,	
      bank_txn_reference as TRANSACTION_ID,	
      pg_source,	
      (CASE WHEN pg.transaction_status = 'I' THEN 'INITIATED' WHEN pg.transaction_status = 'F' THEN 'FAILED' WHEN pg.transaction_status = 'C' THEN 'CANCELLED' WHEN pg.transaction_status = 'S' THEN 'SUCCESS' END) AS PAYMENT_STATUS,	
      txn_initiated_date,
      txn_completed_date,	
      pgd_gateway_id,	
      gateway_name,	
      default_currency,
      allowed_currencies,	
      txn_charge_fixed,	
      txn_charge_percentage,	
      gateway_mode,	
      status,	
      created_at, 
      updated_at	 
    from base_pg_transaction pg
    inner join base_payment_gateway_details pgd 
    on pg.pg_gateway_id = pgd.pgd_gateway_id
)


select * from final
order by pg_booking_master_id

