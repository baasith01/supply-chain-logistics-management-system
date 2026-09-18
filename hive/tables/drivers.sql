-- Hive table: drivers
-- Database: logistics_db
--
-- Purpose:
--   Stores driver master data and performance-related attributes.
--
-- Source:
--   data/raw/drivers.csv

CREATE DATABASE IF NOT EXISTS logistics_db;

USE logistics_db;

CREATE TABLE IF NOT EXISTS drivers (
    driver_id STRING,
    driver_name STRING,
    region STRING,
    experience_years INT,
    rating DOUBLE,
    employment_status STRING,
    license_type STRING
)
COMMENT 'Driver master and performance data for logistics analytics'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Example load from HDFS:
--
-- LOAD DATA INPATH
-- '/user/<hdfs_user>/logistics/raw/drivers.csv'
-- INTO TABLE drivers;

-- Basic validation queries:
--
-- SELECT COUNT(*) FROM drivers;
--
-- SELECT employment_status,
--        COUNT(*) AS driver_count
-- FROM drivers
-- GROUP BY employment_status
-- ORDER BY driver_count DESC;
--
-- SELECT region,
--        COUNT(*) AS driver_count,
--        ROUND(AVG(rating), 2) AS avg_rating,
--        ROUND(AVG(experience_years), 2) AS avg_experience_years
-- FROM drivers
-- GROUP BY region
-- ORDER BY driver_count DESC;
--
-- SELECT license_type,
--        COUNT(*) AS driver_count
-- FROM drivers
-- GROUP BY license_type
-- ORDER BY driver_count DESC;
