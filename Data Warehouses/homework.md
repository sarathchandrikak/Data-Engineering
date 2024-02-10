## Week 3 Homework
ATTENTION: At the end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. This repository should contain your code for solving the homework. If your solution includes code that is not in file format (such as SQL queries or shell commands), please include these directly in the README file of your repository.

<b><u>Important Note:</b></u> <p> For this homework we will be using the 2022 Green Taxi Trip Record Parquet Files from the New York
City Taxi Data found here: </br> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page </br>
If you are using orchestration such as Mage, Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You will need to use the PARQUET option files when creating an External Table</br>

<b>SETUP:</b></br>
Create an external table using the Green Taxi Trip Records Data for 2022. </br>
Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table). </br>
</p>

## Question 1:
Question 1: What is count of records for the 2022 Green Taxi Data??
- 65,623,481
- 840,402
- 1,936,423
- 253,647

```sql

SELECT COUNT(*) FROM stone-passage-413413.ny_taxi.green_data_2022;

```

![Data](https://github.com/sarathchandrikak/Data-Engineering/blob/main/Data%20Warehouses/imgs/question_1.png)


## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 6.41MB for the Materialized Table
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table


```sql
## Create Materialized table

CREATE TABLE stone-passage-413413.ny_taxi.materialized_green_2022
OPTIONS (
  description = 'Materialized version of green taxi 2022 data'
) AS
SELECT
  *
FROM
  stone-passage-413413.ny_taxi.green_data_2022;


## Extract from Normal table

SELECT COUNT(DISTINCT PULocationID) FROM stone-passage-413413.ny_taxi.green_data_2022;


## Extract from Materialized table

SELECT COUNT(DISTINCT PULocationID) FROM stone-passage-413413.ny_taxi.materialized_green_2022;
```

![Data](https://github.com/sarathchandrikak/Data-Engineering/blob/main/Data%20Warehouses/imgs/question_2.png)


## Question 3:
How many records have a fare_amount of 0?
- 12,488
- 128,219
- 112
- 1,622

```sql

   SELECT COUNT(*) FROM stone-passage-413413.ny_taxi.green_data_2022 WHERE fare_amount = 0;

```

![Data](https://github.com/sarathchandrikak/Data-Engineering/blob/main/Data%20Warehouses/imgs/question_3.png)


## Question 4:
What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
- Cluster on lpep_pickup_datetime Partition by PUlocationID
- Partition by lpep_pickup_datetime  Cluster on PUlocationID
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

```sql

 # Since ordering needs all data with respect to PULocationID, it is advised to cluster by PULocationID and filter condition need only specific record/records, so partitioning on lpep_pickup_datetime. So for the above scenario, the best strategy is to partition the table by lpep_pickup_datetime and cluster it by PUlocationID.

CREATE TABLE stone-passage-413413.ny_taxi.green_2022_partitioned
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID
AS
SELECT
  *
FROM
  stone-passage-413413.ny_taxi.green_data_2022;
```

## Question 5:
Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime
06/01/2022 and 06/30/2022 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table



```sql

# Extraction from normal table

SELECT DISTINCT PULocationID FROM stone-passage-413413.ny_taxi.green_data_2022 WHERE lpep_pickup_datetime >= '2022-06-01' AND lpep_pickup_datetime <= '2022-06-30';
```

```sql

# Extraction from partitioned table

SELECT DISTINCT PULocationID FROM stone-passage-413413.ny_taxi.green_2022_partitioned WHERE lpep_pickup_datetime >= '2022-06-01' AND lpep_pickup_datetime <= '2022-06-30';
```

#### External/Normal Table

![Data](https://github.com/sarathchandrikak/Data-Engineering/blob/main/Data%20Warehouses/imgs/external_table.png)


#### Partitioned Table

![Data](https://github.com/sarathchandrikak/Data-Engineering/blob/main/Data%20Warehouses/imgs/partitioned_space.png)


## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- Big Table
- Container Registry


      Data gets stored in Big Query


## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- False

      False. Tables are clustered based on the use case. 
