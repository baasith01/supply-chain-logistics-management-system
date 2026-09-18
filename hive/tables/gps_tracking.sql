-- Hive table: gps_tracking
-- Database: logistics_db
--
-- Purpose:
--   Stores vehicle GPS telemetry used for real-time fleet monitoring,
--   route analysis, utilization analysis, and operational intelligence.
--
-- Source:
--   data/raw/gps_tracking.csv

CREATE DATABASE IF NOT EXISTS logistics_db;

USE logistics_db;

CREATE TABLE IF NOT EXISTS gps_tracking (
    vehicle_id STRING,
    timestamp TIMESTAMP,
    region STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    speed_kmph DOUBLE,
    heading DOUBLE,
    vehicle_status STRING,
    ignition_status STRING,
    signal_quality STRING
)
COMMENT 'Vehicle GPS telemetry and movement data'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Example load from HDFS:
--
-- LOAD DATA INPATH
-- '/user/<hdfs_user>/logistics/raw/gps_tracking.csv'
-- INTO TABLE gps_tracking;

-- Basic validation queries:
--
-- SELECT COUNT(*) FROM gps_tracking;
--
-- SELECT region,
--        COUNT(*) AS gps_records,
--        ROUND(AVG(speed_kmph), 2) AS avg_speed_kmph
-- FROM gps_tracking
-- GROUP BY region
-- ORDER BY gps_records DESC;
--
-- Vehicle movement analysis:
--
-- SELECT vehicle_id,
--        ROUND(AVG(speed_kmph), 2) AS avg_speed_kmph,
--        ROUND(MAX(speed_kmph), 2) AS max_speed_kmph,
--        COUNT(*) AS telemetry_records
-- FROM gps_tracking
-- GROUP BY vehicle_id
-- ORDER BY avg_speed_kmph DESC;
--
-- Signal quality monitoring:
--
-- SELECT signal_quality,
--        COUNT(*) AS record_count
-- FROM gps_tracking
-- GROUP BY signal_quality
-- ORDER BY record_count DESC;
--
-- Operational status analysis:
--
-- SELECT vehicle_status,
--        ignition_status,
--        COUNT(*) AS record_count
-- FROM gps_tracking
-- GROUP BY vehicle_status, ignition_status
-- ORDER BY record_count DESC;
--
-- Geographic data validation:
--
-- SELECT COUNT(*) AS invalid_coordinates
-- FROM gps_tracking
-- WHERE latitude IS NULL
--    OR longitude IS NULL
--    OR latitude < -90
--    OR latitude > 90
--    OR longitude < -180
--    OR longitude > 180;
