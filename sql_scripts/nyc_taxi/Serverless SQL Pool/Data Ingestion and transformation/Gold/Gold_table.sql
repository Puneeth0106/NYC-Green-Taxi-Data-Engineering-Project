use nyc_taxi_ldw

---Requirement-1

-- select Top 100
--        td.year,
--        td.month,
--        Convert(Date,td.lpep_pickup_datetime) as trip_date,
--        cal.day_name,
--        tz.borough,
--        case when cal.day_name in ('Saturday','Sunday') Then 'Y' Else 'N' END as trip_day_weekend_ind,
--        SUM(case when pt.description= 'Credit card' then 1 else 0 End) as card_trip_count,
--        SUM(case when pt.description= 'Cash' then 1 else 0 end ) as cash_trip_count
-- from silver.vw_trip_data_green td
-- join silver.taxi_zone  tz on (tz.LocationID =td.PULocationID)
-- join silver.calendar cal on (Convert(Date,td.lpep_pickup_datetime)=cal.date)
-- join silver.payment_type pt  on (td.payment_type=pt.payment_type)
-- Group by td.year,
--        td.month,
--        Convert(Date,td.lpep_pickup_datetime),
--        tz.borough,
--        cal.day_name



---- requirement-2

select Top 1
       td.year,
       td.month,
       Convert(Date,td.lpep_pickup_datetime) as trip_date,
       cal.day_name,
       tz.borough,
       case when cal.day_name in ('Saturday','Sunday') Then 'Y' Else 'N' END as trip_day_weekend_ind,
       sum(td.trip_distance) trip_distance,
       sum(td.fare_amount) Total_fare_amount,
       SUM(case when pt.description= 'Credit card' then 1 else 0 End) as card_trip_count,
       SUM(case when pt.description= 'Cash' then 1 else 0 end ) as cash_trip_count,
       SUM(DATEDIFF(minute, td.lpep_pickup_datetime, td.lpep_dropoff_datetime)) AS trip_duration_minutes,
       SUM( case when btd.trip_type_desc='Dispatch' then 1 else 0 end) as Despatch_trip_count,
       SUM( case when btd.trip_type_desc='Street-hail' then 1 else 0 end) as Street_hail_trip_count
from silver.vw_trip_data_green td
join silver.taxi_zone  tz on (tz.LocationID =td.PULocationID)
join silver.calendar cal on (Convert(Date,td.lpep_pickup_datetime)=cal.date)
join silver.payment_type pt  on (td.payment_type=pt.payment_type)
join bronze.trip_data btd on (btd.trip_type=td.trip_type)
Group by td.year,
       td.month,
       Convert(Date,td.lpep_pickup_datetime),
       tz.borough,
       cal.day_name



select Top 10* from silver.vw_trip_data_green

select Top 10* from bronze.trip_data

EXEC gold.usp_gold_trip_data_green_create_and_drop '2020', '02';
EXEC gold.usp_gold_trip_data_green_create_and_drop '2020', '03';
EXEC gold.usp_gold_trip_data_green_create_and_drop '2020', '04';
EXEC gold.usp_gold_trip_data_green_create_and_drop '2020', '05';
EXEC gold.usp_gold_trip_data_green_create_and_drop '2020', '06';
EXEC gold.usp_gold_trip_data_green_create_and_drop '2020', '07';
EXEC gold.usp_gold_trip_data_green_create_and_drop '2020', '08';
EXEC gold.usp_gold_trip_data_green_create_and_drop '2020', '09';
EXEC gold.usp_gold_trip_data_green_create_and_drop '2020', '10';
EXEC gold.usp_gold_trip_data_green_create_and_drop '2020', '11';
EXEC gold.usp_gold_trip_data_green_create_and_drop '2020', '12';
EXEC gold.usp_gold_trip_data_green_create_and_drop '2021', '01';
EXEC gold.usp_gold_trip_data_green_create_and_drop '2021', '02';
EXEC gold.usp_gold_trip_data_green_create_and_drop '2021', '03';
EXEC gold.usp_gold_trip_data_green_create_and_drop '2021', '04';
EXEC gold.usp_gold_trip_data_green_create_and_drop '2021', '05';
EXEC gold.usp_gold_trip_data_green_create_and_drop '2021', '06';
EXEC gold.usp_gold_trip_data_green_create_and_drop '2021', '07';

