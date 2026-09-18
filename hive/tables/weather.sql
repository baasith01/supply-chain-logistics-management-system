-- Hive table: weather
-- Database: logistics_db
--
-- Purpose:
--   Stores weather observations used to analyze weather impact on deliveries,
--   route performance, and operational risk.
--
-- Source:
--   data/raw/weather.csv

CREATE DATABASE IF NOT EXISTS logistics_db;

USE logistics_db;

CREATE TABLE IF NOT EXISTS weather (
    date DATE,
    location STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    temperature_c DOUBLE,
    humidity_pct DOUBLE,
    rainfall_mm DOUBLE,
    weather_condition STRING
)
COMMENT 'Weather observations for logistics and delivery analysis'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Example load from HDFS:
--
-- LOAD DATA INPATH
-- '/user/<hdfs_user>/logistics/raw/weather.csv'
-- INTO TABLE weather;

-- Basic validation queries:
--
-- SELECT COUNT(*) FROM weather;
--
-- SELECT location,
--        COUNT(*) AS observations,
--        ROUND(AVG(temperature_c), 2) AS avg_temperature_c,
--        ROUND(AVG(rainfall_mm), 2) AS avg_rainfall_mm
-- FROM weather
-- GROUP BY location
-- ORDER BY observations DESC;
--
-- Weather-condition analysis:
--
-- SELECT weather_condition,
--        COUNT(*) AS observation_count,
--        ROUND(AVG(rainfall_mm), 2) AS avg_rainfall_mm,
--        ROUND(AVG(humidity_pct), 2) AS avg_humidity_pct
-- FROM weather
-- GROUP BY weather_condition
-- ORDER BY observation_count DESC;
--
-- Rainfall risk:
--
-- SELECT location,
--        SUM(rainfall_mm) AS total_rainfall_mm,
--        MAX(rainfall_mm) AS max_rainfall_mm
-- FROM weather
-- GROUP BY location
-- ORDER BY total_rainfall_mm DESC;
--
-- Geographic validation:
--
-- SELECT COUNT(*) AS invalid_coordinates
-- FROM weather
-- WHERE latitude IS NULL
--    OR longitude IS NULL
--    OR latitude < -90
--    OR latitude > 90
--    OR longitude < -180
--    OR longitude > 180;
