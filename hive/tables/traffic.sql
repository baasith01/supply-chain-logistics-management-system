-- Hive table: traffic
-- Database: logistics_db
--
-- Purpose:
--   Stores traffic observations used for route analysis, delivery-delay
--   analysis, and logistics operational intelligence.
--
-- Source:
--   data/raw/traffic.csv

CREATE DATABASE IF NOT EXISTS logistics_db;

USE logistics_db;

CREATE TABLE IF NOT EXISTS traffic (
    timestamp TIMESTAMP,
    location STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    traffic_score DOUBLE,
    traffic_level STRING,
    average_speed_kmph DOUBLE
)
COMMENT 'Traffic observations for logistics and route analysis'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Example load from HDFS:
--
-- LOAD DATA INPATH
-- '/user/<hdfs_user>/logistics/raw/traffic.csv'
-- INTO TABLE traffic;

-- Basic validation queries:
--
-- SELECT COUNT(*) FROM traffic;
--
-- SELECT traffic_level,
--        COUNT(*) AS observation_count,
--        ROUND(AVG(traffic_score), 2) AS avg_traffic_score,
--        ROUND(AVG(average_speed_kmph), 2) AS avg_speed_kmph
-- FROM traffic
-- GROUP BY traffic_level
-- ORDER BY observation_count DESC;
--
-- Location-level traffic analysis:
--
-- SELECT location,
--        COUNT(*) AS observations,
--        ROUND(AVG(traffic_score), 2) AS avg_traffic_score,
--        ROUND(AVG(average_speed_kmph), 2) AS avg_speed_kmph
-- FROM traffic
-- GROUP BY location
-- ORDER BY avg_traffic_score DESC;
--
-- High-traffic periods:
--
-- SELECT
--     YEAR(timestamp) AS year,
--     MONTH(timestamp) AS month,
--     DAY(timestamp) AS day,
--     HOUR(timestamp) AS hour,
--     ROUND(AVG(traffic_score), 2) AS avg_traffic_score,
--     ROUND(AVG(average_speed_kmph), 2) AS avg_speed_kmph
-- FROM traffic
-- GROUP BY
--     YEAR(timestamp),
--     MONTH(timestamp),
--     DAY(timestamp),
--     HOUR(timestamp)
-- ORDER BY avg_traffic_score DESC;
--
-- Geographic validation:
--
-- SELECT COUNT(*) AS invalid_coordinates
-- FROM traffic
-- WHERE latitude IS NULL
--    OR longitude IS NULL
--    OR latitude < -90
--    OR latitude > 90
--    OR longitude < -180
--    OR longitude > 180;
