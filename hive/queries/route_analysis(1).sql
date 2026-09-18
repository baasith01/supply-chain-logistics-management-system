-- Hive analytical queries: route_analysis
-- Database: logistics_db
--
-- Purpose:
--   Analyze route efficiency using delivery, GPS, and traffic information.
--
-- Note:
--   The current raw orders dataset contains delivery distance but does not
--   contain a route_id. Therefore, route performance is analyzed using
--   region, distance, vehicle movement, and traffic conditions rather than
--   inventing a direct order-to-road relationship.

USE logistics_db;

-- ============================================================
-- 1. Delivery distance by region
-- ============================================================

SELECT
    region,
    COUNT(*) AS total_orders,
    ROUND(AVG(delivery_distance_km), 2) AS avg_distance_km,
    ROUND(MIN(delivery_distance_km), 2) AS min_distance_km,
    ROUND(MAX(delivery_distance_km), 2) AS max_distance_km
FROM orders
GROUP BY region
ORDER BY avg_distance_km DESC;


-- ============================================================
-- 2. Delivery performance by distance bucket
-- ============================================================

SELECT
    CASE
        WHEN delivery_distance_km < 5 THEN '0-5 km'
        WHEN delivery_distance_km < 10 THEN '5-10 km'
        WHEN delivery_distance_km < 20 THEN '10-20 km'
        WHEN delivery_distance_km < 50 THEN '20-50 km'
        ELSE '50+ km'
    END AS distance_bucket,
    COUNT(*) AS total_orders,
    ROUND(AVG(promised_delivery_time_min), 2) AS avg_promised_time_min,
    ROUND(AVG(actual_delivery_time_min), 2) AS avg_actual_time_min,
    ROUND(
        AVG(actual_delivery_time_min - promised_delivery_time_min),
        2
    ) AS avg_time_variance_min
FROM orders
GROUP BY
    CASE
        WHEN delivery_distance_km < 5 THEN '0-5 km'
        WHEN delivery_distance_km < 10 THEN '5-10 km'
        WHEN delivery_distance_km < 20 THEN '10-20 km'
        WHEN delivery_distance_km < 50 THEN '20-50 km'
        ELSE '50+ km'
    END;


-- ============================================================
-- 3. Regional GPS movement performance
-- ============================================================

SELECT
    region,
    COUNT(*) AS gps_records,
    COUNT(DISTINCT vehicle_id) AS tracked_vehicles,
    ROUND(AVG(speed_kmph), 2) AS avg_speed_kmph,
    ROUND(MAX(speed_kmph), 2) AS max_speed_kmph
FROM gps_tracking
GROUP BY region
ORDER BY avg_speed_kmph DESC;


-- ============================================================
-- 4. Vehicle movement summary
-- ============================================================

SELECT
    vehicle_id,
    COUNT(*) AS telemetry_records,
    ROUND(AVG(speed_kmph), 2) AS avg_speed_kmph,
    ROUND(MAX(speed_kmph), 2) AS max_speed_kmph,
    SUM(
        CASE
            WHEN speed_kmph > 0 THEN 1
            ELSE 0
        END
    ) AS moving_records,
    SUM(
        CASE
            WHEN speed_kmph = 0 THEN 1
            ELSE 0
        END
    ) AS stationary_records
FROM gps_tracking
GROUP BY vehicle_id
ORDER BY avg_speed_kmph DESC;


-- ============================================================
-- 5. Traffic conditions by location
-- ============================================================

SELECT
    location,
    COUNT(*) AS observations,
    ROUND(AVG(traffic_score), 2) AS avg_traffic_score,
    ROUND(AVG(average_speed_kmph), 2) AS avg_traffic_speed_kmph,
    ROUND(MAX(traffic_score), 2) AS max_traffic_score
FROM traffic
GROUP BY location
ORDER BY avg_traffic_score DESC;


-- ============================================================
-- 6. Traffic-level impact
-- ============================================================

SELECT
    traffic_level,
    COUNT(*) AS observations,
    ROUND(AVG(traffic_score), 2) AS avg_traffic_score,
    ROUND(AVG(average_speed_kmph), 2) AS avg_speed_kmph
FROM traffic
GROUP BY traffic_level
ORDER BY avg_traffic_score DESC;


-- ============================================================
-- 7. Hourly traffic pattern
-- ============================================================

SELECT
    HOUR(timestamp) AS hour_of_day,
    COUNT(*) AS observations,
    ROUND(AVG(traffic_score), 2) AS avg_traffic_score,
    ROUND(AVG(average_speed_kmph), 2) AS avg_speed_kmph
FROM traffic
GROUP BY HOUR(timestamp)
ORDER BY hour_of_day;


-- ============================================================
-- 8. Potential congestion hotspots
-- ============================================================

SELECT
    location,
    ROUND(AVG(traffic_score), 2) AS avg_traffic_score,
    ROUND(AVG(average_speed_kmph), 2) AS avg_speed_kmph,
    COUNT(*) AS observations
FROM traffic
GROUP BY location
HAVING AVG(traffic_score) >= 70
ORDER BY avg_traffic_score DESC;


-- ============================================================
-- 9. Region-level delivery efficiency
-- ============================================================

SELECT
    region,
    COUNT(*) AS total_orders,
    ROUND(AVG(delivery_distance_km), 2) AS avg_distance_km,
    ROUND(AVG(actual_delivery_time_min), 2) AS avg_delivery_time_min,
    ROUND(
        AVG(
            CASE
                WHEN delivery_distance_km > 0
                THEN actual_delivery_time_min / delivery_distance_km
                ELSE NULL
            END
        ),
        2
    ) AS avg_minutes_per_km
FROM orders
GROUP BY region
ORDER BY avg_minutes_per_km;


-- ============================================================
-- 10. Regional delivery and traffic comparison
-- ============================================================
-- orders.region and traffic.location use compatible project location labels.
-- Aggregate each dataset first to avoid multiplying rows during the join.

WITH delivery_region AS (
    SELECT
        region,
        COUNT(*) AS total_orders,
        ROUND(AVG(delivery_distance_km), 2) AS avg_delivery_distance_km,
        ROUND(AVG(actual_delivery_time_min), 2) AS avg_delivery_time_min,
        ROUND(
            AVG(actual_delivery_time_min - promised_delivery_time_min),
            2
        ) AS avg_delivery_variance_min
    FROM orders
    GROUP BY region
),
traffic_region AS (
    SELECT
        location,
        ROUND(AVG(traffic_score), 2) AS avg_traffic_score,
        ROUND(AVG(average_speed_kmph), 2) AS avg_traffic_speed_kmph
    FROM traffic
    GROUP BY location
)
SELECT
    d.region,
    d.total_orders,
    d.avg_delivery_distance_km,
    d.avg_delivery_time_min,
    d.avg_delivery_variance_min,
    t.avg_traffic_score,
    t.avg_traffic_speed_kmph
FROM delivery_region d
LEFT JOIN traffic_region t
    ON d.region = t.location
ORDER BY t.avg_traffic_score DESC;


-- ============================================================
-- 11. Route-operation KPI summary
-- ============================================================

SELECT
    COUNT(*) AS total_orders,
    ROUND(AVG(delivery_distance_km), 2) AS avg_delivery_distance_km,
    ROUND(SUM(delivery_distance_km), 2) AS total_delivery_distance_km,
    ROUND(AVG(actual_delivery_time_min), 2) AS avg_delivery_time_min,
    ROUND(
        AVG(
            CASE
                WHEN delivery_distance_km > 0
                THEN actual_delivery_time_min / delivery_distance_km
                ELSE NULL
            END
        ),
        2
    ) AS avg_minutes_per_km
FROM orders;
