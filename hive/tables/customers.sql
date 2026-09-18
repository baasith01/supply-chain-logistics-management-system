-- Hive table: customers
-- Database: logistics_db
--
-- Purpose:
--   Stores customer master data used for customer-level logistics analytics.
--
-- Source:
--   data/raw/customers.csv

CREATE DATABASE IF NOT EXISTS logistics_db;

USE logistics_db;

CREATE TABLE IF NOT EXISTS customers (
    customer_id STRING,
    customer_name STRING,
    region STRING,
    customer_type STRING,
    registration_date DATE
)
COMMENT 'Customer master data for logistics analytics'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Example load from HDFS:
--
-- LOAD DATA INPATH
-- '/user/<hdfs_user>/logistics/raw/customers.csv'
-- INTO TABLE customers;

-- Basic validation queries:
--
-- SELECT COUNT(*) FROM customers;
--
-- SELECT customer_type, COUNT(*) AS customer_count
-- FROM customers
-- GROUP BY customer_type
-- ORDER BY customer_count DESC;
--
-- SELECT region, COUNT(*) AS customer_count
-- FROM customers
-- GROUP BY region
-- ORDER BY customer_count DESC;
--
-- SELECT customer_type,
--        ROUND(AVG(DATEDIFF(CURRENT_DATE, registration_date)), 0)
--            AS avg_customer_age_days
-- FROM customers
-- GROUP BY customer_type;
