# Stream Processing 

# Topics Covered

* Why Stream Processing 
* Rising Wave
* Data Ingestion and Delivery
* Staless / Stateful Computation 

### Batch Vs Stream 

| Batch Processing | Stream Processing | 
|---|---|
| Batch data is static | Stream data is mostly dynamic with high volume of data | 
| Frequency can be hourly or weekly or monthly | Frequency can be every minute or second |
| Common sources are files or databases | Common sources are IOT devices, Social Media Feeds |


### What is RisingWave?

RisingWave is a distributed SQL streaming database that enables simple, efficient, and reliable processing of streaming data. It is open-source db, allows developers to perform stream processing in the same way as they operate a database. Developers can express intricate processing logic through materialized views, and query materailized views to get always up-to-date and consistent results.

* Users can use RisingWave for:

Stream processing
Data storage
Random querying, especially point queries


* Users should not use RisingWave for：

Transaction processing
Ad-hoc analytical queries that involve frequent full table scans


### Rising Wave Architecture

<img width="1189" alt="Screenshot 2024-03-10 at 4 31 07 PM" src="https://github.com/sarathchandrikak/Data-Engineering/assets/65502906/ef4be3b4-25bf-4fa7-88e4-d083b42536bd">


### Rising Wave 
<img width="630" alt="Screenshot 2024-03-10 at 4 33 24 PM" src="https://github.com/sarathchandrikak/Data-Engineering/assets/65502906/598fb019-7543-4acd-a4fe-5de7050f329c">




