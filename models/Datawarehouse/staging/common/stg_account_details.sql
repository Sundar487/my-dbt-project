with 

account_details_table as (

    select 
        account_id as ad_account_id,
        account_name as ad_account_name,
        parent_account_id as ad_parent_account_id,
        agency_name as ad_agency_name,
        agency_url as ad_agency_url
    from {{ source('clarity_qa', 'account_details') }}

)


select * from account_details_table