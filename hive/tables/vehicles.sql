-- Hive table: vehicles
-- Database: logistics_db
--
-- Purpose:
--   Stores vehicle and fleet information used for fleet intelligence,
--   maintenance analysis, and delivery operations.
--
-- Source:
--   data/raw/vehicles.csv

CREATE DATABASE IF NOT EXISTS logistics_db;

USE logistics_db;

CREATE TABLE IF NOT EXISTS vehicles (
    vehicle_id STRING,
    vehicle_type STRING,
    driver_id STRING,
    fuel_type STRING,
    capacity_kg DOUBLE,
    manufacture_year INT,
    maintenance_status STRING,
    vehicle_status STRING,
    odometer_km DOUBLE
)
COMMENT 'Vehicle and fleet master data for logistics analytics'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Example load from HDFS:
--
-- LOAD DATA INPATH
-- '/user/<hdfs_user>/logistics/raw/vehicles.csv'
-- INTO TABLE vehicles;

-- Basic validation queries:
--
-- SELECT COUNT(*) FROM vehicles;
--
-- SELECT vehicle_type,
--        COUNT(*) AS vehicle_count,
--        ROUND(AVG(capacity_kg), 2) AS avg_capacity_kg
-- FROM vehicles
-- GROUP BY vehicle_type
-- ORDER BY vehicle_count DESC;
--
-- SELECT fuel_type,
--        COUNT(*) AS vehicle_count
-- FROM vehicles
-- GROUP BY fuel_type
-- ORDER BY vehicle_count DESC;
--
-- SELECT maintenance_status,
--        COUNT(*) AS vehicle_count
-- FROM vehicles
-- GROUP BY maintenance_status
-- ORDER BY vehicle_count DESC;
--
-- SELECT vehicle_status,
--        COUNT(*) AS vehicle_count
-- FROM vehicles
-- GROUP BY vehicle_status
-- ORDER BY vehicle_count DESC;
--
-- Fleet utilization / condition example:
--
-- SELECT vehicle_type,
--        ROUND(AVG(odometer_km), 2) AS avg_odometer_km,
--        ROUND(AVG(capacity_kg), 2) AS avg_capacity_kg
-- FROM vehicles
-- GROUP BY vehicle_type;
