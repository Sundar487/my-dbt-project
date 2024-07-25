with 

status_details_table as (
    select *
    from {{ source('clarity_qa', 'status_details') }}
)

select * from status_details_table