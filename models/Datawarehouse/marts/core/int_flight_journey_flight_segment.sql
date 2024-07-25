with 

stg_flight_journey as (

    select * from {{ ref('stg_flight_journey') }}

),

int_flight_segment_1 as (
    SELECT 
        fs_flight_journey_id,
        STRING_AGG(fs_departure_airport, ',') AS fs_departure_airport,
        STRING_AGG(fs_arrival_airport, ',') AS fs_arrival_airport,
        STRING_AGG(fs_flight_segment_id::TEXT, ',') AS fs_flight_segment_id,
        STRING_AGG(row_num::TEXT, ',') AS row_num,
        STRING_AGG(fare_basis_code, ',') AS fare_basis_code,
        STRING_AGG(booking_class, ',') AS booking_class,
        STRING_AGG(airline_code, ',') AS airline_code,
        STRING_AGG(airline_name, ',') AS airline_name,
        STRING_AGG(departure_date_time::TEXT, ',') AS departure_date_time
    FROM (
        SELECT 
            fs_flight_journey_id,
            fs_departure_airport,
            fs_arrival_airport,
            fs_flight_segment_id,		
            fare_basis_code,
            booking_class,
            airline_code,
            airline_name,
            departure_date_time,
            ROW_NUMBER() OVER (PARTITION BY fs_flight_journey_id ORDER BY fs_flight_segment_id) AS row_num
        FROM 
            stg_flight_segment
    ) subquery

    GROUP BY 
        fs_flight_journey_id
    order by fs_flight_journey_id, row_num
),


int_flight_segment_2 as (
    SELECT 
        fs_flight_journey_id,
        departure_date_time,

        CASE WHEN TRIM(SPLIT_PART(fs_flight_segment_id, ',', 1)) <> '' THEN TRIM(SPLIT_PART(fs_flight_segment_id, ',', 1)) ELSE NULL END AS fs_flight_segment_id1,
        CASE WHEN TRIM(SPLIT_PART(fs_departure_airport, ',', 1)) <> '' THEN TRIM(SPLIT_PART(fs_departure_airport, ',', 1)) ELSE NULL END AS origin1,
        CASE WHEN TRIM(SPLIT_PART(fs_arrival_airport, ',', 1)) <> '' THEN TRIM(SPLIT_PART(fs_arrival_airport, ',', 1)) ELSE NULL END AS destination1,
        CASE WHEN TRIM(SPLIT_PART(fare_basis_code, ',', 1)) <> '' THEN TRIM(SPLIT_PART(fare_basis_code, ',', 1)) ELSE NULL END AS fare_basis_code1,
        CASE WHEN TRIM(SPLIT_PART(booking_class, ',', 1)) <> '' THEN TRIM(SPLIT_PART(booking_class, ',', 1)) ELSE NULL END AS booking_class1,
        CASE WHEN TRIM(SPLIT_PART(airline_code, ',', 1)) <> '' THEN TRIM(SPLIT_PART(airline_code, ',', 1)) ELSE NULL END AS airline_code1,
        CASE WHEN TRIM(SPLIT_PART(airline_name, ',', 1)) <> '' THEN TRIM(SPLIT_PART(airline_name, ',', 1)) ELSE NULL END AS airline_name1,
        CASE WHEN TRIM(SPLIT_PART(departure_date_time, ',', 1)) <> '' THEN TRIM(SPLIT_PART(departure_date_time, ',', 1)) ELSE NULL END AS departure_date_time1,
        
        
        CASE WHEN TRIM(SPLIT_PART(fs_flight_segment_id, ',', 2)) <> '' THEN TRIM(SPLIT_PART(fs_flight_segment_id, ',', 2)) ELSE NULL END AS fs_flight_segment_id2,
        CASE WHEN TRIM(SPLIT_PART(fs_departure_airport, ',', 2)) <> '' THEN TRIM(SPLIT_PART(fs_departure_airport, ',', 2)) ELSE NULL END AS origin2,
        CASE WHEN TRIM(SPLIT_PART(fs_arrival_airport, ',', 2)) <> '' THEN TRIM(SPLIT_PART(fs_arrival_airport, ',', 2)) ELSE NULL END AS destination2,
        CASE WHEN TRIM(SPLIT_PART(fare_basis_code, ',', 2)) <> '' THEN TRIM(SPLIT_PART(fare_basis_code, ',', 2)) ELSE NULL END AS fare_basis_code2,
        CASE WHEN TRIM(SPLIT_PART(booking_class, ',', 2)) <> '' THEN TRIM(SPLIT_PART(booking_class, ',', 2)) ELSE NULL END AS booking_class2,
        CASE WHEN TRIM(SPLIT_PART(airline_code, ',', 2)) <> '' THEN TRIM(SPLIT_PART(airline_code, ',', 2)) ELSE NULL END AS airline_code2,
        CASE WHEN TRIM(SPLIT_PART(airline_name, ',', 2)) <> '' THEN TRIM(SPLIT_PART(airline_name, ',', 2)) ELSE NULL END AS airline_name2,
        CASE WHEN TRIM(SPLIT_PART(departure_date_time, ',', 2)) <> '' THEN TRIM(SPLIT_PART(departure_date_time, ',', 2)) ELSE NULL END AS departure_date_time2,
        
        CASE WHEN TRIM(SPLIT_PART(fs_flight_segment_id, ',', 3)) <> '' THEN TRIM(SPLIT_PART(fs_flight_segment_id, ',', 3)) ELSE NULL END AS fs_flight_segment_id3,
        CASE WHEN TRIM(SPLIT_PART(fs_departure_airport, ',', 3)) <> '' THEN TRIM(SPLIT_PART(fs_departure_airport, ',', 3)) ELSE NULL END AS origin3,
        CASE WHEN TRIM(SPLIT_PART(fs_arrival_airport, ',', 3)) <> '' THEN TRIM(SPLIT_PART(fs_arrival_airport, ',', 3)) ELSE NULL END AS destination3,
        CASE WHEN TRIM(SPLIT_PART(fare_basis_code, ',', 3)) <> '' THEN TRIM(SPLIT_PART(fare_basis_code, ',', 3)) ELSE NULL END AS fare_basis_code3,
        CASE WHEN TRIM(SPLIT_PART(booking_class, ',', 3)) <> '' THEN TRIM(SPLIT_PART(booking_class, ',', 3)) ELSE NULL END AS booking_class3,
        CASE WHEN TRIM(SPLIT_PART(airline_code, ',', 3)) <> '' THEN TRIM(SPLIT_PART(airline_code, ',', 3)) ELSE NULL END AS airline_code3,
        CASE WHEN TRIM(SPLIT_PART(airline_name, ',', 3)) <> '' THEN TRIM(SPLIT_PART(airline_name, ',', 3)) ELSE NULL END AS airline_name3,
        CASE WHEN TRIM(SPLIT_PART(departure_date_time, ',', 3)) <> '' THEN TRIM(SPLIT_PART(departure_date_time, ',', 3)) ELSE NULL END AS departure_date_time3,
        
        CASE WHEN TRIM(SPLIT_PART(fs_flight_segment_id, ',', 4)) <> '' THEN TRIM(SPLIT_PART(fs_flight_segment_id, ',', 4)) ELSE NULL END AS fs_flight_segment_id4,
        CASE WHEN TRIM(SPLIT_PART(fs_departure_airport, ',', 4)) <> '' THEN TRIM(SPLIT_PART(fs_departure_airport, ',', 4)) ELSE NULL END AS origin4,
        CASE WHEN TRIM(SPLIT_PART(fs_arrival_airport, ',', 4)) <> '' THEN TRIM(SPLIT_PART(fs_arrival_airport, ',', 4)) ELSE NULL END AS destination4,
        CASE WHEN TRIM(SPLIT_PART(fare_basis_code, ',', 4)) <> '' THEN TRIM(SPLIT_PART(fare_basis_code, ',', 4)) ELSE NULL END AS fare_basis_code4,
        CASE WHEN TRIM(SPLIT_PART(booking_class, ',', 4)) <> '' THEN TRIM(SPLIT_PART(booking_class, ',', 4)) ELSE NULL END AS booking_class4,
        CASE WHEN TRIM(SPLIT_PART(airline_code, ',', 4)) <> '' THEN TRIM(SPLIT_PART(airline_code, ',', 4)) ELSE NULL END AS airline_code4,
        CASE WHEN TRIM(SPLIT_PART(airline_name, ',', 4)) <> '' THEN TRIM(SPLIT_PART(airline_name, ',', 4)) ELSE NULL END AS airline_name4,
        CASE WHEN TRIM(SPLIT_PART(departure_date_time, ',', 4)) <> '' THEN TRIM(SPLIT_PART(departure_date_time, ',', 4)) ELSE NULL END AS departure_date_time4

    FROM int_flight_segment_1
),


int_flight_segment_3 as (

    SELECT 
        fs_flight_journey_id,
        CAST(fs_flight_segment_id1 AS INTEGER) AS fs_flight_segment_id1,
        origin1,
        destination1,
        fare_basis_code1,
        booking_class1,
        airline_code1,
        airline_name1,
        CASE WHEN departure_date_time1 IS NOT NULL THEN CAST(departure_date_time1 AS TIMESTAMP) END AS departure_date_time1,
        CAST(fs_flight_segment_id2 AS INTEGER) AS fs_flight_segment_id2,
        origin2,
        destination2,
        fare_basis_code2,
        booking_class2,
        airline_code2,
        airline_name2,
        CASE WHEN departure_date_time2 IS NOT NULL THEN CAST(departure_date_time2 AS TIMESTAMP) END AS departure_date_time2,
        CAST(fs_flight_segment_id3 AS INTEGER) AS fs_flight_segment_id3,
        origin3,
        destination3,
        fare_basis_code3,
        booking_class3,
        airline_code3,
        airline_name3,
        CASE WHEN departure_date_time3 IS NOT NULL THEN CAST(departure_date_time3 AS TIMESTAMP) END AS departure_date_time3,
        CAST(fs_flight_segment_id4 AS INTEGER) AS fs_flight_segment_id4,
        origin4,
        destination4,
        fare_basis_code4,
        booking_class4,
        airline_code4,
        airline_name4,
        CASE WHEN departure_date_time4 IS NOT NULL THEN CAST(departure_date_time4 AS TIMESTAMP) END AS departure_date_time4
    
    FROM 
        int_flight_segment_2 

),


final as (
    select * from stg_flight_journey fj  inner join int_flight_segment_3 fs
    on fs.fs_flight_journey_id = fj.fj_flight_journey_id
)

select * from final

