with 


payment_gateway_details_table as (
    select
        gateway_id as pgd_gateway_id,	
        portal_id as pgd_portal_id,	
        gateway_name,	
        default_currency,	
        allowed_currencies,
        txn_charge_fixed, 
        txn_charge_percentage, 
        gateway_mode, 
        status,	
        created_at, 
        updated_at  
    from {{ source('clarity_qa', 'payment_gateway_details') }}
)


select * from payment_gateway_details_table
