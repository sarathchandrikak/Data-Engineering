# Apache Hive - AWS

# Input Data 

Steps to create upload data into S3 bucket and create external tables: 

 1. Copy data from one S3 bucket to your s3 bucket 
 
        aws s3 cp s3://input_data s3://yourbucketname/yourfoldername/ 
        
 2. After copying open hive terminal to execute hive queries
        
 3. Create a db for storing

        create database amz_review;
        
 4. Create an external table with name amz_review_dataset and load data into it
 
        create external table amz_review.amz_review_dump (json_dump string)  location 's3a://<your_bucket_name>/tables/';
  
 5. Add data to different columns

        create external table amz_review.amz_review_col (
          reviewerid string,
          asin string,
          reviewername string,
          helpful array<int>,
          reviewtext string,
          overall double,
          summary string,
          unixreviewtime bigint)
          row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
          with serdeproperties ('paths'= '')
          location 's3a://<your bucket name> /tables/';
          
After performing above steps external table amz_review_col gets created for analysis. 

# Data Analysis Without Partition

1. Comparision between count( * ) and count( 1 ) execution
         
         Select count(*) from amz_review_col; 
         
         Select count(1) from amz_review_col;
         
2. Time execution for distinct count 

        Select count (distinct asin) am_products from amz_review_col;
        
3. Time execution for group by operation 

       Select rate_year , count(1) cnt from (
       Select year(from_unixtime(unixreviewtime)) rate_year from amz_review_col )T group by rate_year order by rate_year desc;
 
 # Data Analysis with Partition 
 
   To have paritions certain flags need to be enabled in Apache Hive. Following are the flags 
   
        
