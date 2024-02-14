# BigQuery Model Building


Data has been imported into GCS using [script](https://github.com/sarathchandrikak/Data-Engineering/blob/main/Data%20Warehouses/extract_yellow_trip.py) for the yellow taxi trip data for years 2019, 2020. 

Original dataset location: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page


#### 📣 Create table using taxi data

```sql
CREATE OR REPLACE EXTERNAL TABLE `stone-passage-413413.yellow_taxi.data` 
OPTIONS (
format = 'parquet',
uris = ['gs://data-warehouse-ip/yellow/downloads/*']
);
```


#### 📣 Create partitioned table 

```sql
CREATE OR REPLACE TABLE `stone-passage-413413.yellow_taxi.data_partitioned` 
PARTITION BY DATE(tpep_pickup_datetime)
AS 
SELECT
*
FROM
stone-passage-413413.yellow_taxi.data;
```


#### 📣 Select requrired columns from partitioned table
```sql
SELECT
    passenger_count,
    trip_distance,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    tolls_amount,
    tip_amount
FROM `stone-passage-413413.yellow_taxi.data_partitioned` WHERE fare_amount != 0;
```


#### 📣 Create ML table

```sql
-- CREATE A ML TABLE WITH APPROPRIATE TYPE
CREATE OR REPLACE TABLE `stone-passage-413413.yellow_taxi.data_ml` (
`passenger_count` FLOAT64,
`trip_distance` FLOAT64,
`PULocationID` STRING,
`DOLocationID` STRING,
`payment_type` STRING,
`fare_amount` FLOAT64,
`tolls_amount` FLOAT64,
`tip_amount` FLOAT64
) AS (
SELECT passenger_count, trip_distance, cast(PULocationID AS STRING), CAST(DOLocationID AS STRING),
CAST(payment_type AS STRING), fare_amount, tolls_amount, tip_amount
FROM `stone-passage-413413.yellow_taxi.data_partitioned` WHERE fare_amount != 0
);
```


### 📣 Create ML model with Default setting

```sql

-- CREATE MODEL WITH DEFAULT SETTING
CREATE OR REPLACE MODEL `stone-passage-413413.yellow_taxi.tip_model`
OPTIONS
(model_type='linear_reg',
input_label_cols=['tip_amount'],
DATA_SPLIT_METHOD='AUTO_SPLIT') AS
SELECT
*
FROM
`stone-passage-413413.yellow_taxi.data_ml`
WHERE
tip_amount IS NOT NULL;
```

#### 📣 Check feature info
```sql
-- CHECK FEATURES
SELECT * FROM ML.FEATURE_INFO(MODEL `stone-passage-413413.yellow_taxi.tip_model`);
```

#### 📣 Evaluate the model
```sql
-- EVALUATE THE MODEL
SELECT
*
FROM
ML.EVALUATE(MODEL `stone-passage-413413.yellow_taxi.tip_model`,
(
SELECT
*
FROM
`stone-passage-413413.yellow_taxi.data_ml`
WHERE
tip_amount IS NOT NULL
));
```

![img](https://github.com/sarathchandrikak/Data-Engineering/blob/main/Data%20Warehouses/imgs/model_evaluate.png)

#### 📣 Predict the model

```sql
-- PREDICT THE MODEL
SELECT
*
FROM
ML.PREDICT(MODEL `stone-passage-413413.yellow_taxi.tip_model`,
(
SELECT
*
FROM
`stone-passage-413413.yellow_taxi.data_ml`
WHERE
tip_amount IS NOT NULL
));
```

![img](https://github.com/sarathchandrikak/Data-Engineering/blob/main/Data%20Warehouses/imgs/actualvspredicted.png)

#### 📣 Predict and Explain the model
```sql

-- PREDICT AND EXPLAIN
SELECT
*
FROM
ML.EXPLAIN_PREDICT(MODEL `stone-passage-413413.yellow_taxi.tip_model`,
(
SELECT
*
FROM
`stone-passage-413413.yellow_taxi.data_ml`
WHERE
tip_amount IS NOT NULL
), STRUCT(3 as top_k_features));
```

#### 📣 Additionally define hyper parameters and run the model
```sql
-- HYPER PARAM TUNNING
CREATE OR REPLACE MODEL `stone-passage-413413.yellow_taxi.tip_hyperparam_model`
OPTIONS
(model_type='linear_reg',
input_label_cols=['tip_amount'],
DATA_SPLIT_METHOD='AUTO_SPLIT',
num_trials=5,
max_parallel_trials=2,
l1_reg=hparam_range(0, 20),
l2_reg=hparam_candidates([0, 0.1, 1, 10])) AS
SELECT
*
FROM
`stone-passage-413413.yellow_taxi.data_ml`
WHERE
tip_amount IS NOT NULL;
```
