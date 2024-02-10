-- Creating an external table with options
CREATE OR REPLACE EXTERNAL TABLE `stone-passage-413413.ny_taxi.green_data_2022`  OPTIONS (
format = 'parquet',
uris = ['gs://data-warehouse-ip/green_2022/downloads/*']
);

-- Question 1:

SELECT COUNT(*) FROM stone-passage-413413.ny_taxi.green_data_2022;


-- Question 2:

-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

SELECT COUNT(DISTINCT PULocationID) FROM stone-passage-413413.ny_taxi.green_data_2022;

CREATE TABLE stone-passage-413413.ny_taxi.materialized_green_2022
OPTIONS (
  description = 'Materialized version of green taxi 2022 data'
) AS
SELECT
  *
FROM
  stone-passage-413413.ny_taxi.green_data_2022;

SELECT COUNT(DISTINCT PULocationID) FROM stone-passage-413413.ny_taxi.materialized_green_2022;


-- Question 3:

-- How many records have a fare_amount of 0?

SELECT COUNT(*) FROM stone-passage-413413.ny_taxi.green_data_2022 WHERE fare_amount = 0;


-- Question 4:

-- What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

-- Partition by lpep_pickup_datetime Cluster on PUlocationID


-- Question 5:

-- Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

-- Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

CREATE TABLE stone-passage-413413.ny_taxi.green_2022_partitioned
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID
AS
SELECT
  *
FROM
  stone-passage-413413.ny_taxi.green_data_2022;

SELECT DISTINCT PULocationID FROM stone-passage-413413.ny_taxi.green_data_2022 WHERE lpep_pickup_datetime > '2022-06-01' AND lpep_pickup_datetime < '2022-06-30';

SELECT DISTINCT PULocationID FROM stone-passage-413413.ny_taxi.green_2022_partitioned WHERE lpep_pickup_datetime > '2022-06-01' AND lpep_pickup_datetime < '2022-06-30';


-- Question 8:

-- No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

SELECT COUNT(*) FROM stone-passage-413413.ny_taxi.materialized_green_2022;