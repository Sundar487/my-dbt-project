{{
  config(
    materialized = 'table',
  )
}}

with 

car_itinerary as (
    select 
      ci_booking_master_id, 
      ci_booking_req_id, 
      ci_car_itinerary_id, 
      car_type, 
      car_make, 
      passengers, 
      mileage, 
      fuel_type, 
      transmission,
      provider_name, 
      supplier_name, 
      pickup_location, 
      dropoff_location, 
      rental_days,
      date(pickup_date) as ci_pickup_date,
      dropoff_date
    from {{ ref('stg_car_itinerary') }}
)

select * from car_itinerary









