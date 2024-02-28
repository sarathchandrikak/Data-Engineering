import pandas as pd
import os 

work_dir = os.getcwd()
yellow_sample = pd.read_csv(f"{work_dir}/downloads/fhv_tripdata_2019-01.csv")

print(yellow_sample.head())

# 'VendorID': pd.Int64Dtype(),
#         'passenger_count': pd.Int64Dtype(),
#         'trip_distance': float,
#         'RatecodeID': pd.Int64Dtype(),
#         'store_and_fwd_flag': str,
#         'PULocationID': pd.Int64Dtype(),
#         'DOLocationID': pd.Int64Dtype(),
#         'payment_type': pd.Int64Dtype(),
#         'fare_amount': float,
#         'extra': float,
#         'mta_tax': float,
#         'tip_amount': float,
#         'tolls_amount': float,
#         'improvement_surcharge': float,
#         'total_amount': float,
#         'congestion_surcharge': float, 
#         'tpep_pickup_datetime': DATETIME,
#         'tpep_dropoff_datetime': DATETIME

# print(yellow_sample.columns)
breakpoint()

# yellow_sample['airport_fee'].value_counts()


#  -- payment info
#     cast(fare_amount as numeric) as fare_amount,
#     cast(extra as numeric) as extra,
#     cast(mta_tax as numeric) as mta_tax,
#     cast(tip_amount as numeric) as tip_amount,
#     cast(tolls_amount as numeric) as tolls_amount,
#     cast(ehail_fee as numeric) as ehail_fee,
#     cast(improvement_surcharge as numeric) as improvement_surcharge,
#     cast(total_amount as numeric) as total_amount,
#     coalesce({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }},0) as payment_type,
#     {{ get_payment_type_description("payment_type") }} as payment_type_description




# CREATE OR REPLACE EXTERNAL TABLE `stone-passage-413413.dbt_sk.green_data` (
#   /* New datetime columns with CAST */
#   `lpep_pickup_datetime` TIMESTAMP,
#   `lpep_dropoff_datetime` TIMESTAMP,
#   `VendorID` INT64, 
#   `passenger_count` INT64,
#   `trip_distance` FLOAT64,
#   `RatecodeID` INT64,
#   `store_and_fwd_flag` STRING,
#   `PULocationID` INT64,
#   `DOLocationID` INT64,
#   `payment_type` INT64,
#   `fare_amount` INT64,
#   `extra` FLOAT64,
#   `mta_tax` FLOAT64,
#   `tip_amount` FLOAT64,
#   `tolls_amount` FLOAT64,
#   `improvement_surcharge` FLOAT64,
#   `total_amount` FLOAT64,
#   `congestion_surcharge` FLOAT64,
#   `trip_type` FLOAT64,
# )
# OPTIONS (
#   format = `parquet`,
#   uris = ['gs://data-warehouse-ip/taxi_data/green/*']  /* Assuming corrections are made */
# );



# CREATE OR REPLACE EXTERNAL TABLE `stone-passage-413413.dbt_sk.green_data`  OPTIONS (
# format = `parquet`,
# uris = [`gs://data-warehouse-ip/taxi_data/green/*`]
# );




# CREATE OR REPLACE EXTERNAL TABLE `stone-passage-413413.dbt_sk.green_data` (
#   /* New datetime columns with CAST */
#   `lpep_pickup_datetime` TIMESTAMP,
#   `lpep_dropoff_datetime` TIMESTAMP,
#   `VendorID` INT64, 
#   `passenger_count` INT64,
#   `trip_distance` FLOAT64,
#   `RatecodeID` INT64,
#   `store_and_fwd_flag` STRING,
#   `PULocationID` INT64,
#   `DOLocationID` INT64,
#   `payment_type` INT64,
#   `fare_amount` FLOAT64,
#   `extra` FLOAT64,
#   `mta_tax` FLOAT64,
#   `tip_amount` FLOAT64,
#   `tolls_amount` FLOAT64,
#   `improvement_surcharge` FLOAT64,
#   `total_amount` FLOAT64,
#   `congestion_surcharge` FLOAT64,
#   `trip_type` FLOAT64,
# )
# OPTIONS (
#   format = `parquet`,
#   uris = ['gs://data-warehouse-ip/taxi_data/green/*']  /* Assuming corrections are made */
# );



# CREATE OR REPLACE EXTERNAL TABLE `stone-passage-413413.dbt_sk.yellow_data` (
#   /* New datetime columns with CAST */
#   `tpep_pickup_datetime` TIMESTAMP,
#   `tpep_dropoff_datetime` TIMESTAMP,
#   `VendorID` INT64, 
#   `passenger_count` INT64,
#   `trip_distance` FLOAT64,
#   `RatecodeID` INT64,
#   `store_and_fwd_flag` STRING,
#   `PULocationID` INT64,
#   `DOLocationID` INT64,
#   `payment_type` INT64,
#   `fare_amount` FLOAT64,
#   `extra` FLOAT64,
#   `mta_tax` FLOAT64,
#   `tip_amount` FLOAT64,
#   `tolls_amount` FLOAT64,
#   `improvement_surcharge` FLOAT64,
#   `total_amount` FLOAT64,
#   `congestion_surcharge` FLOAT64,
#   `trip_type` FLOAT64,
# )
# OPTIONS (
#   format = `parquet`,
#   uris = ['gs://data-warehouse-ip/taxi_data/yellow/*']  /* Assuming corrections are made */
# );


# ```sql

# CREATE OR REPLACE EXTERNAL TABLE `stone-passage-413413.dbt_sk.yellow_data`  OPTIONS (
# format = `parquet`,
# uris = [`gs://data-warehouse-ip/taxi_data/yellow/*`]
# );

# SELECT FORMAT_TIMESTAMP (`lpep_pickup_datetime`) from `stone-passage-413413.dbt_sk.green_data` ;

# SELECT CAST(`lpep_pickup_datetime` AS TIMESTAMP) AS datetime_column
# FROM `stone-passage-413413.dbt_sk.green_data`;
# ```


df = pd.read_csv("fhv_tripdata_2019-01.csv")
print(df.head())