with 

cer_snapshot as (
    
    select
        exchange_rate_id,
        type,
        exchange_rate_from_currency,
        exchange_rate_to_currency,
        exchange_rate_equivalent_value,
        status,
        created_at as currency_exchange_rate_created_at,
        updated_at as currency_exchange_rate_updated_at,
        EXTRACT(MONTH FROM updated_at)as cer_month,
        EXTRACT(YEAR FROM updated_at) as cer_year,
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to  
    from {{ ref('currency_exchange_rate_snapshot') }}
    where exchange_rate_to_currency = 'CAD' and status = 'A'

),

temp1 as (

    select avg(exchange_rate_equivalent_value), max(exchange_rate_id)  from cer_snapshot
    group by exchange_rate_from_currency, cer_year, cer_month

),


temp2 as (

    select * from  cer_snapshot scer inner join temp1 t1 
    on scer.exchange_rate_id = t1.max
    
)

select * from temp2


