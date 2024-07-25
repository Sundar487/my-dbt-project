{% snapshot currency_exchange_rate_snapshot %}

{{
   config(
       target_schema='snapshots',
       unique_key="exchange_rate_from_currency||'-'||exchange_rate_to_currency||'-'||exchange_rate_equivalent_value",

       strategy='timestamp',
       updated_at='updated_at',
   )
}}

select * from {{ source('clarity_qa', 'currency_exchange_rate') }}

{% endsnapshot %}


