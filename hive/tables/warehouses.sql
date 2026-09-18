-- Hive table: warehouses
-- Database: logistics_db
--
-- Purpose:
--   Stores warehouse master data, geographic coordinates, and capacity
--   information used for warehouse and network analysis.
--
-- Source:
--   data/raw/warehouses.csv

CREATE DATABASE IF NOT EXISTS logistics_db;

USE logistics_db;

CREATE TABLE IF NOT EXISTS warehouses (
    warehouse_id STRING,
    warehouse_name STRING,
    region STRING,
    capacity_units INT,
    latitude DOUBLE,
    longitude DOUBLE
)
COMMENT 'Warehouse master, capacity, and location data'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Example load from HDFS:
--
-- LOAD DATA INPATH
-- '/user/<hdfs_user>/logistics/raw/warehouses.csv'
-- INTO TABLE warehouses;

-- Basic validation queries:
--
-- SELECT COUNT(*) FROM warehouses;
--
-- SELECT region,
--        COUNT(*) AS warehouse_count,
--        SUM(capacity_units) AS total_capacity_units
-- FROM warehouses
-- GROUP BY region
-- ORDER BY total_capacity_units DESC;
--
-- SELECT warehouse_id,
--        warehouse_name,
--        capacity_units
-- FROM warehouses
-- ORDER BY capacity_units DESC;
--
-- Geographic validation:
--
-- SELECT warehouse_id,
--        warehouse_name,
--        latitude,
--        longitude
-- FROM warehouses
-- WHERE latitude IS NULL
--    OR longitude IS NULL;
--
-- Capacity analysis:
--
-- SELECT
--     SUM(capacity_units) AS total_network_capacity,
--     ROUND(AVG(capacity_units), 2) AS avg_warehouse_capacity,
--     MIN(capacity_units) AS minimum_capacity,
--     MAX(capacity_units) AS maximum_capacity
-- FROM warehouses;
