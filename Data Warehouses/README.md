# Data Warehoues


# BIG QUERY



## Big Query Best Practice 

Best practices are mostly focused on cost reduction or performance improvement

#### Steps to consider for cost reduction
🔹 Avoid Select * and instead specify column name, this is because of the columnar architecture of Big Query \
🔹 Clustering and Partitioning help in reducing cost \
🔹 Materialize query into different stages 

#### Steps to consider for performance imporvement 
🔹 Filter on partitioned/cluster column \
🔹 Denormalize the data \
🔹 Reduce data before using a join \
🔹 Do not treat WITH clauses as prepared statements \
🔹 Avoid Overshading tables 

#### General Considerations
🔹 Avoid using javascript in user defined functions \
🔹 Use approximate aggregation functions (HyperLogLog++) \
🔹 Place the table with largest rows as the first one while joining 


## Big Query Internals 

🔹 Big Query stores the data in Colossus - a columnar storage which is very cheap \
🔹 Also since Big Query is separate from compute, costs are minimal \
🔹 Most cost occured is during the read/ executing the query \
🔹 Jupiter Network is inside Big Query data centers and provides 1TB/s speed and communicate without any delays \
🔹 Distribution of workers and dividing the query into smaller chunks help in fast fetching/executing of the data 


## Big Query ML
