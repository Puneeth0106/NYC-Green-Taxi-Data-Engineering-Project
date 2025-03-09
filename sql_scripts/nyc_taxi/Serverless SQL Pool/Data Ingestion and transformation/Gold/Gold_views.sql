drop view if EXISTS gold.vw_trip_data_green 
go

create view gold.vw_trip_data_green
AS
SELECT 
        result.filepath(1) as year,
        result.filepath(2) as month,
        result.* 
From OPENROWSET(
        BULK 'gold/trip_data_green/year=*/month=*/*.parquet',
        Data_source = 'nyc_taxi_raw',
        Format = 'Parquet'
)with (
        borough varchar(20),
        trip_date Date,
        trip_day varchar(10),
        trip_day_weekend_ind VARCHAR(10),
        card_trip_count SMALLINT,
        cash_trip_count SMALLINT,
        Street_hail_trip_count SMALLINT,
        Despatch_trip_count SMALLINT,
        trip_duration_minutes INT,
        trip_distance FLOAT,
        Total_fare_amount FLOAT
) as result


select * from gold.vw_trip_data_green
where year=2020 and month = 01



