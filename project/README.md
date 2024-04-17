# Data Engineering Pipeline

# 🔺 Problem Statement

Examine the crowd-sourced data collection regarding mobile telephone network coverage in Catalonia. Develop a thorough data pipeline focused on extracting insights from US data is used to analyze the quality of mobile coverage in Catalonia of the four main operators (Movistar, Vodafone, Orange and Yoigo) and filter data according to the technology used (2G, 3G or 4G) spanning over the years 2015 to 2017

# 🔺 Dataset

Dataset is from Big Query public datasets availabe [here](https://console.cloud.google.com/marketplace/product/gencat/cell_coverage?project=data-engineering-final-418904)

![img](https://github.com/sarathchandrikak/Data-Engineering/blob/main/project/data-info.png)

Dataset can also be accessed from big query using this command 

```sql
SELECT * FROM `bigquery-public-data.catalonian_mobile_coverage.mobile_data_2015_2017` LIMIT 1000
```

# 🔺 Architecture

![img](https://github.com/sarathchandrikak/Data-Engineering/blob/main/project/ip_pic.drawio.png)


# 🔺 Implementation Steps


🔺 Create a new table in our project based on partition and clustering by appropriate columns in BigQuery 

```sql
CREATE TABLE data-engineering-final-418904.mobile_coverage.us_data (
  date date,
hour TIME,	
lat	FLOAT64,	
long FLOAT64,	
signal INT64,	
network STRING,	
operator STRING,	
status INT64,	
description STRING,	
net STRING,	
speed	FLOAT64,	
satellites FLOAT64,	
precission FLOAT64,	
provider STRING,	
activity STRING,	
postal_code STRING,	
town_name STRING	
)
PARTITION BY
  date
CLUSTER BY
  postal_code;
```

🔺 Insert data from the publicly available datasets to our newly created table

```sql
INSERT INTO data-engineering-final-418904.mobile_coverage.us_data (  date,
  hour,
  lat,
  long,
  signal,
  network,
  operator,
  status,
  description,
  net,
  speed,
  satellites,
  precission,
  provider,
  activity,
  postal_code,
  town_name)
SELECT   date,
  hour,
  lat,
  long,
  signal,
  network,
  operator,
  status,
  description,
  net,
  speed,
  satellites,
  precission,
  provider,
  activity,
  postal_code,
  town_name
FROM bigquery-public-data.catalonian_mobile_coverage.mobile_data_2015_2017;
```

🔺 Perform analysis on Dataproc cluster as per the code from data_analysis_sql.py using the following command. It reads the file from google cloud storage and perform analysis by running spark job on the cluster

      
    gcloud dataproc jobs submit pyspark  
      --cluster=cluster-eb9d       
      --region=us-central1       
      --properties=spark.sql.legacy.timeParserPolicy=LEGACY       
      gs://de_mobile_data/data_analysis_sql.py 

# 🔺 Dashboard

![img](https://github.com/sarathchandrikak/Data-Engineering/blob/main/project/dashboard.png)


# 🔺 Results
