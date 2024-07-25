{{
  config(
    materialized = 'table'
    )
}}

with 

date_dim_table as (
  {{ dbt_date.get_date_dimension("2018-01-01", "2024-12-31") }}
),

date_dim as (
    select 
--        {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_key,
        date_day as date,
        year_number as year,
        quarter_of_year as quarter,
        month_name,
        month_of_year as month,
        day_of_month as day,
        week_of_year as week

    from date_dim_table
)

select * from date_dim
order by date




