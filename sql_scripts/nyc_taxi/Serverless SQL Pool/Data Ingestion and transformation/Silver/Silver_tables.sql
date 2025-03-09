use nyc_taxi_ldw
---- Converting from csv to parquet

select * from bronze.calendar

if object_id('silver.calendar') is not NULL
    drop EXTERNAL table silver.calendar
CREATE EXTERNAL TABLE silver.calendar
WITH(
    LOCATION ='silver/calendar',
    DATA_SOURCE = nyc_taxi_raw,
    FILE_FORMAT = parquet_file_format
)
AS
SELECT *
FROM bronze.calendar

select * from silver.calendar

---- Converting from csv(taxi_zone) to parquet 

if object_id(silver.taxi_zone) is NOT NULL
    drop EXTERNAL table silver.taxi_zone
    Go

    CREATE EXTERNAL TABLE silver.taxi_zone
    With(
        LOCATION= 'silver/taxi_zone',
        DATA_SOURCE = nyc_taxi_raw,
        FILE_FORMAT = parquet_file_format
    )
    AS 
    SELECT *
    from bronze.taxi_zone


select * from silver.taxi_zone



---- Tranforming from json to parquet
if object_id('silver.rate_code') is not NULL
    drop external table silver.rate_code

Create external table silver.rate_code
with(
    LOCATION = 'silver/rate_code',
    DATA_SOURCE =nyc_taxi_raw,
    FILE_FORMAT= parquet_file_format
)
AS
select *
from bronze.vw_rate_code


select * from silver.rate_code


---- Tranforming from json to parquet

CREATE EXTERNAL TABLE silver.payment_type
with(
    LOCATION ='silver/payment_type',
    DATA_SOURCE = nyc_taxi_raw,
    FILE_FORMAT= parquet_file_format
)
AS
SELECT
    payment_type,
    description
FROM
    OPENROWSET(
        BULK 'raw/payment_type.json',
        DATA_SOURCE ='nyc_taxi_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '1.0',
        HEADER_ROW=TRUE,
        FIELDTERMINATOR='0X0b',
        FIELDQUOTE='0X0b'
    )WITH(
            jsondoc NVARCHAR(max)
    ) as Payment_type
CROSS APPLY OPENJSON(jsondoc)
    WITH(
        payment_type SMALLINT,
        description Varchar(20) '$.payment_type_desc'
    )


select * from silver.payment_type

---- Transforming csv folder to parquet file

Create external table silver.trip_data_green
with(
    LOCATION ='silver/trip_data_green',
    DATA_SOURCE = nyc_taxi_raw,
    FILE_FORMAT = parquet_file_format
)
AS
select *
from bronze.trip_data_green



---- Creating external table for trip_data

if object_id('silver.trip_type') is not NULL
    drop EXTERNAL table silver.trip_type

CREATE EXTERNAL TABLE silver.trip_type
with(
	Location ='silver/trip_type',
    DATA_SOURCE = nyc_taxi_raw,
    FILE_FORMAT = parquet_file_format
) AS
select *
from bronze.trip_data


---- creating external table for vendor data

if object_id('silver.vendor') is not NULL
    drop EXTERNAL table silver.vendor

CREATE EXTERNAL TABLE silver.vendor
with(
	Location ='silver/vendor',
    DATA_SOURCE = nyc_taxi_raw,
    FILE_FORMAT = parquet_file_format
) AS
select *
from bronze.vendor_data


select * from silver.vendor




