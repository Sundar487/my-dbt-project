with 

portal_details_table as (
    select 
        portal_id as pd_portal_id, 
        portal_name,
        portal_url,
        account_id as pd_account_id,
        parent_portal_id,
        agency_name as pd_agency_name, 
        agency_city,
        agency_country, 
        agency_zipcode, 
        agency_mobile_code, 
        agency_mobile_code_country,
        agency_mobile, 
        agency_phone, 
        prime_country, 
        business_type, 
        portal_default_currency,
        portal_selling_currencies, 
        portal_settlement_currencies

    from {{ source('clarity_qa', 'portal_details') }}
)


select * from portal_details_table