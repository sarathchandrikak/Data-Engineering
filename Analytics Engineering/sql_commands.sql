 CREATE OR REPLACE EXTERNAL TABLE `stone-passage-413413.dbt_sk.fhv_data`  OPTIONS (
 format = `parquet`,
 uris = ['gs://data-warehouse-ip/taxi_data/fhv/*']
 );

 select max(SR_flag) from `stone-passage-413413.dbt_sk.fhv_data`;

 CREATE OR REPLACE EXTERNAL TABLE `stone-passage-413413.dbt_sk.yellow_data` (
  `tpep_pickup_datetime` INT64,
  `tpep_dropoff_datetime` INT64,
  `VendorID` INT64, 
  `passenger_count` INT64,
  `trip_distance` FLOAT64,
  `RatecodeID` INT64,
  `store_and_fwd_flag` STRING,
  `PULocationID` INT64,
  `DOLocationID` INT64,
  `payment_type` INT64,
  `fare_amount` FLOAT64,
  `extra` FLOAT64,
  `mta_tax` FLOAT64,
  `tip_amount` FLOAT64,
  `tolls_amount` FLOAT64,
  `improvement_surcharge` FLOAT64,
  `total_amount` FLOAT64,
  `congestion_surcharge` FLOAT64,
  `trip_type` FLOAT64,
)
OPTIONS (
  format = `parquet`,
  uris = ['gs://data-warehouse-ip/taxi_data/yellow/*']
);

 CREATE OR REPLACE EXTERNAL TABLE `stone-passage-413413.dbt_sk.green_data` (
  `lpep_pickup_datetime` INT64,
  `lpep_dropoff_datetime` INT64,
  `VendorID` INT64, 
  `passenger_count` INT64,
  `trip_distance` FLOAT64,
  `RatecodeID` INT64,
  `store_and_fwd_flag` STRING,
  `PULocationID` INT64,
  `DOLocationID` INT64,
  `payment_type` INT64,
  `fare_amount` FLOAT64,
  `extra` FLOAT64,
  `mta_tax` FLOAT64,
  `tip_amount` FLOAT64,
  `tolls_amount` FLOAT64,
  `improvement_surcharge` FLOAT64,
  `total_amount` FLOAT64,
  `congestion_surcharge` FLOAT64,
  `trip_type` FLOAT64,
)
OPTIONS (
  format = `parquet`,
  uris = ['gs://data-warehouse-ip/taxi_data/green/*']
);

select count(*) from `stone-passage-413413.dbt_sk.yellow_data`;

select count(*) from `stone-passage-413413.dbt_sk.fhv_fact_trips`;




