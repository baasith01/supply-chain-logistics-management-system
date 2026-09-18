-- Hive table: orders
-- Database: logistics_db
--
-- Purpose:
--   Stores order-level logistics and delivery information.
--
-- Source:
--   data/raw/orders.csv
--
-- This table is designed for analytical workloads in Hive/Spark.

CREATE DATABASE IF NOT EXISTS logistics_db;

USE logistics_db;

CREATE TABLE IF NOT EXISTS orders (
    order_id STRING,
    customer_id STRING,
    warehouse_id STRING,
    driver_id STRING,
    vehicle_id STRING,
    order_date DATE,
    region STRING,
    order_status STRING,
    delivery_status STRING,
    order_value DECIMAL(12,2),
    delivery_distance_km DOUBLE,
    promised_delivery_time_min INT,
    actual_delivery_time_min INT
)
COMMENT 'Order-level logistics and delivery data'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Example load from the project raw data directory.
-- Adjust the path according to the HDFS location used in the environment.
--
-- LOAD DATA INPATH
-- '/user/<hdfs_user>/logistics/raw/orders.csv'
-- INTO TABLE orders;

-- Basic validation queries:
--
-- SELECT COUNT(*) FROM orders;
--
-- SELECT delivery_status, COUNT(*)
-- FROM orders
-- GROUP BY delivery_status;
--
-- SELECT region,
--        COUNT(*) AS total_orders,
--        ROUND(AVG(order_value), 2) AS avg_order_value,
--        ROUND(AVG(delivery_distance_km), 2) AS avg_distance_km
-- FROM orders
-- GROUP BY region
-- ORDER BY total_orders DESC;
