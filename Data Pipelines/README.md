# Data Pipelines

💠 Data Pipeline is a series of connected process where output of one process is input of the next.\
💠 They move or modify data from one place or form to other.\
💠 Data flow in form of data packets.

# Data Pipeline Performance

💠 Performance is measured by latency, throughput.\
💠 Latency refers to the total time taken for a single packet of data to pass within the pipeline and overall latency is limited by the slowest process in the pipeline.\
💠 Throughput refers to how much data can be fed through the pipeline per unit of time. Prcessing larger packets per unit time increases throughput.

# Use Cases 

💠 Backing up files\
💠 Integrating disparate raw data sources into data lake\
💠 Moving transactional records to a data warehouse\
💠 Streaming data from IoT devices to dashboards\
💠 Preparing raw data fro machine learning development or production\
💠 Messaging systems such as email, SMS, video meetings

# Data Pipeline Stages

💠 Data Extraction\
💠 Data Ingestion\
💠 Transformation Changes\
💠 Loading data into destination facility\
💠 Scheduling or triggering the job to run\
💠 Monitoring Entire workflow\
💠 Maintenance and Optimization of the pipeline

# Pipeline Monitoring Considerations 

💠 Latency\
💠 Throughput\
💠 Warnings, errors, failures \
💠 Utilization rate \
💠 Logging and alerting systems

Unbalanced loads can be handled by introducing parallelization and I/O buffers can help mitigate bottlenecks.

# Batch Pipelines

💠 Batch Pipelines operates on batches of data. They usually runs periodically, hours, days or weeks apart.\
💠 They can be initiated based on data size or other triggers.\
💠 These are used when accuracy is critical.\
💠 By decreasing the batch size and increasing processing speed, we can achieve near real time processing.\
💠 Smaller batches improve load balancing and lower latency.\
💠 Example Use cases for batch data pipelines include: Periodic data backups, transaction history loading, processing of customer orders and billing, data modelling on slowly varying data, Mid- to long-range sales forecasting and weather forecasting, Analysis of historical data, Diagnostic medical image processing.

# Streaming Pipeline

💠 Streaming pipelines operates on records or events processed as they happen. They ingest data packets in rapid succession.\
💠 These are used for real time results.\
💠 Example Use cases for streaming data pipelines include: Watching movies, listening to music or podcasts. Social media feeds and sentiment analysis. Fraud detection. User behavior analysis and targeted advertising. Stock market trading. Real-time product pricing and Recommender systems.



# Lambda Architecture

💠 A Lambda architecture is a hybrid architecture, designed for handling Big Data. Lambda architectures combine batch and streaming data pipeline methods.\
💠 Historical data is delivered in batches to the batch layer, and real-time data is streamed to a speed layer. These two layers are then integrated in the serving layer. \
💠 Lambda can be used in cases where access to earlier data is required but speed is also important. \
💠 A downside to this approach is the complexity involved in the design.\
💠 Lambda architecture is chosen when aiming for both accuracy and speed.

