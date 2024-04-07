import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage


# Ensure correct project ID
client = bigquery.Client(project='data-engineering-final-418904') 

dataset_id = "data-engineering-final-418904.network_coverage"

# Authenticate with the credentials
# client = storage.Client(credentials=credentials)
dataset = bigquery.Dataset(dataset_id)
bqcliet = bigquery.Client()
dataset.location = "us-central1"

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Analysis") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

df = spark.read.format('bigquery') \
    .option('table', 'data-engineering-final-418904.mobile_coverage.data') \
    .load()

df = df.dropna(how="any")
df = df.withColumn("year", F.year(F.col("date")))
df = df.withColumn("month", F.month(F.col("date")))

# Create a temporary view for DataFrame
df.createOrReplaceTempView("data_table")

# Analyze network coverage by counting occurrences of each network type using Spark SQL
network_coverage_query = """
    SELECT 
        network, 
        COUNT(*) AS count 
    FROM 
        data_table 
    GROUP BY 
        network
    ORDER BY 
        count DESC
"""

network_coverage = spark.sql(network_coverage_query)

network_coverage.write.format('bigquery') \
    .option('table', 'data-engineering-final-418904.mobile_coverage.network_coverage') \
    .option('temporaryGcsBucket', 'rankings-inf') \
    .mode('overwrite').save()

# Analyze customer activity and network status using Spark SQL
customer_experience_query = """
    SELECT 
        activity, 
        status, 
        COUNT(*) AS count
    FROM 
        data_table 
    GROUP BY 
        activity, status
"""

customer_experience = spark.sql(customer_experience_query)

customer_experience.write.format('bigquery') \
    .option('table', 'data-engineering-final-418904.mobile_coverage.customer_experience') \
    .option('temporaryGcsBucket', 'rankings-inf') \
    .mode('overwrite').save()

agg_info = """select year, month, network, net, operator, description, max(satellites) as max_satellite, 
    max(speed) as max_speed, 
    max(precission) as max_precision, 
    max(signal) as max_signal, 
    avg(satellites) as avg_satellite, 
    avg(speed) as avg_speed, 
    avg(precission) as avg_precision, 
    avg(signal) as avg_signal from data_table group by 1, 2, 3, 4, 5, 6"""

df_agg = spark.sql(agg_info)

df_agg.write.format('bigquery') \
    .option('table', 'data-engineering-final-418904.mobile_coverage.aggregated_info') \
    .option('temporaryGcsBucket', 'rankings-inf') \
    .mode('overwrite').save()



town_details = """select  town_name, postal_code, operator , activity, count(*) as count from data_table group by 1,2,3, 4"""
df_town = spark.sql(town_details)

df_town.write.format('bigquery') \
    .option('table', 'data-engineering-final-418904.mobile_coverage.town_info') \
    .option('temporaryGcsBucket', 'rankings-inf') \
    .mode('overwrite').save()

