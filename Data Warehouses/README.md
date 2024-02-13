# ♦️ Data Warehoues

Data Warehouse is a OLAP solution used for reporting and data analysis. It has raw data, meta data, summary data, data marts.

# ♦️ BIG QUERY

A serverless Data Warehouse system. Bigquery maximizes flexibility by separating the compute engine from storage. 


## ♦️ Partitioning

🔹 Mostly done on time unit column. Can be done on hourly, yearly, monthly basis \
🔹 Maximum number of partitions supported is 4000 \
🔹 Partitioning results in small amount of data per partition \
🔹 Partitioning might also results in a large number of partitions beyond the limits on partitioned tables \
🔹 For partitioned table clustering is maintained for data within the scope of partition

## ♦️ Clustering

🔹 Clustering is performed to colocate the data with columns specified \
🔹 Order of the column is important \
🔹 Clustering improves filter and aggregate queries \
🔹 In clustering we can specify upto 4 columns \
🔹 Table with size < 1 GB may not show great difference with clustering and partitioning

## ♦️ Partitioning vs Clustering 

![img](https://github.com/sarathchandrikak/Data-Engineering/blob/main/Data%20Warehouses/imgs/partitioning%20vs%20clustering.png)

## ♦️ Big Query Best Practice 

Best practices are mostly focused on cost reduction or performance improvement

#### ♦️ Steps to consider for cost reduction
🔹 Avoid Select * and instead specify column name, this is because of the columnar architecture of Big Query \
🔹 Clustering and Partitioning help in reducing cost \
🔹 Materialize query into different stages 

#### ♦️ Steps to consider for performance imporvement 
🔹 Filter on partitioned/cluster column \
🔹 Denormalize the data \
🔹 Reduce data before using a join \
🔹 Do not treat WITH clauses as prepared statements \
🔹 Avoid Overshading tables 

#### ♦️ General Considerations
🔹 Avoid using javascript in user defined functions \
🔹 Use approximate aggregation functions (HyperLogLog++) \
🔹 Place the table with largest rows as the first one while joining 

## ♦️ Big Query Internals 

🔹 Big Query stores the data in Colossus - a columnar storage which is very cheap \
🔹 Also since Big Query is separate from compute, costs are minimal \
🔹 Most cost occured is during the read/ executing the query \
🔹 Jupiter Network is inside Big Query data centers and provides 1TB/s speed and communicate without any delays \
🔹 Distribution of workers and dividing the query into smaller chunks help in fast fetching/executing of the data 


## ♦️ Big Query ML

🔹 Pricing is the important factor to decide on ML in Big Query \
🔹 Steps involved in ML model development \
 ![img](https://github.com/sarathchandrikak/Data-Engineering/blob/main/Data%20Warehouses/imgs/ml_flow.png)
🔹 Big Query does automatic feature engineering and hyper parameter tuning \
🔹 It also allows us to deploy models using docker images \ 
