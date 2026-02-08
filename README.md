# 03-Data-Warehosing

## **Question 1. What is count of records for the 2024 Yellow Taxi Data?**

**Answer** : 20,332,093

**code** :
```sql
          SELECT COUNT(*)
          FROM `qwiklabs-gcp-00-2d010dbe49d6.yellow_taxi.external_yellow_tripdata_2024`;
```

## **Question 2. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?**

**Answer** : 0 MB for the External Table and 155.12 MB for the Materialized Table

**code**
```sql
select count(distinct(PULocationID)) from  `qwiklabs-gcp-00-2d010dbe49d6.yellow_taxi.external_yellow_tripdata_2024`;

select count(distinct(PULocationID)) from `qwiklabs-gcp-00-2d010dbe49d6.yellow_taxi.yellow_tripdata_2024`;
```

## **Question 3. Why are the estimated number of Bytes different?**

**answer** : BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

**code** :
```sql 
select PULocationID from `qwiklabs-gcp-00-2d010dbe49d6.yellow_taxi.yellow_tripdata_2024`;
select PULocationID , DOLocationID from `qwiklabs-gcp-00-2d010dbe49d6.yellow_taxi.yellow_tripdata_2024`;
```
					
## **Question 4. How many records have a fare_amount of 0?**

**Answer** : 8,333

**Code** : 
```sql
select count(*) from `qwiklabs-gcp-00-2d010dbe49d6.yellow_taxi.yellow_tripdata_2024` where fare_amount = 0;
```

## **Question 5. What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)**

**Answer**: Partition by tpep_dropoff_datetime and Cluster on VendorID

**Code** : 
```sql
CREATE OR REPLACE TABLE
`qwiklabs-gcp-00-2d010dbe49d6.yellow_taxi.yellow_tripdata_2024_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT *
FROM `qwiklabs-gcp-00-2d010dbe49d6.yellow_taxi.yellow_tripdata_2024`;
```

## **Question 6. Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive). Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?**

**Answer** : 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

**Code**: 
```sql
SELECT DISTINCT VendorID
from `qwiklabs-gcp-00-2d010dbe49d6.yellow_taxi.yellow_tripdata_2024`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'
ORDER BY VendorID;

SELECT DISTINCT VendorID
FROM `qwiklabs-gcp-00-2d010dbe49d6.yellow_taxi.yellow_tripdata_2024_optimized`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'
ORDER BY VendorID;
```

## **Question 7. Where is the data stored in the External Table you created?**

**Answer** : GCP Bucket

## **Question 8. It is best practice in Big Query to always cluster your data:**

**Answer** : TRUE

## **Question 9. Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?**

**Answer** : 0 byte BigQuery counts rows metadata cheaply

**code** 
```sql
select count(*) from `qwiklabs-gcp-00-2d010dbe49d6.yellow_taxi.yellow_tripdata_2024`; --0 MB
select count(*) from `qwiklabs-gcp-00-2d010dbe49d6.yellow_taxi.yellow_tripdata_2024` where Vendor_id = 1; --155.12 MB
```
