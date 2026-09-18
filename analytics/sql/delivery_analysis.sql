-- Hive analytical queries: delivery_analysis
-- Database: logistics_db
--
-- Purpose:
--   Analyze delivery performance, delays, service levels, and operational
--   patterns using the orders table.
--
-- Assumptions:
--   promised_delivery_time_min and actual_delivery_time_min are measured
--   from the same delivery start/reference point.

USE logistics_db;

-- ============================================================
-- 1. Overall delivery performance
-- ============================================================

SELECT
    COUNT(*) AS total_orders,
    ROUND(AVG(order_value), 2) AS avg_order_value,
    ROUND(AVG(delivery_distance_km), 2) AS avg_distance_km,
    ROUND(AVG(promised_delivery_time_min), 2) AS avg_promised_time_min,
    ROUND(AVG(actual_delivery_time_min), 2) AS avg_actual_time_min
FROM orders;


-- ============================================================
-- 2. On-time vs delayed deliveries
-- ============================================================

SELECT
    CASE
        WHEN actual_delivery_time_min <= promised_delivery_time_min
            THEN 'On Time'
        ELSE 'Delayed'
    END AS delivery_performance,
    COUNT(*) AS order_count,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    ) AS percentage
FROM orders
GROUP BY
    CASE
        WHEN actual_delivery_time_min <= promised_delivery_time_min
            THEN 'On Time'
        ELSE 'Delayed'
    END
ORDER BY order_count DESC;


-- ============================================================
-- 3. Average delay by region
-- ============================================================

SELECT
    region,
    COUNT(*) AS total_orders,
    SUM(
        CASE
            WHEN actual_delivery_time_min > promised_delivery_time_min
            THEN 1
            ELSE 0
        END
    ) AS delayed_orders,
    ROUND(
        AVG(
            CASE
                WHEN actual_delivery_time_min > promised_delivery_time_min
                THEN actual_delivery_time_min - promised_delivery_time_min
                ELSE 0
            END
        ),
        2
    ) AS avg_delay_min
FROM orders
GROUP BY region
ORDER BY avg_delay_min DESC;


-- ============================================================
-- 4. Delivery status distribution
-- ============================================================

SELECT
    delivery_status,
    COUNT(*) AS order_count,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    ) AS percentage
FROM orders
GROUP BY delivery_status
ORDER BY order_count DESC;


-- ============================================================
-- 5. Order status distribution
-- ============================================================

SELECT
    order_status,
    COUNT(*) AS order_count
FROM orders
GROUP BY order_status
ORDER BY order_count DESC;


-- ============================================================
-- 6. Delay by delivery-distance bucket
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
    ROUND(AVG(actual_delivery_time_min), 2) AS avg_actual_time_min,
    ROUND(
        AVG(
            CASE
                WHEN actual_delivery_time_min > promised_delivery_time_min
                THEN actual_delivery_time_min - promised_delivery_time_min
                ELSE 0
            END
        ),
        2
    ) AS avg_delay_min
FROM orders
GROUP BY
    CASE
        WHEN delivery_distance_km < 5 THEN '0-5 km'
        WHEN delivery_distance_km < 10 THEN '5-10 km'
        WHEN delivery_distance_km < 20 THEN '10-20 km'
        WHEN delivery_distance_km < 50 THEN '20-50 km'
        ELSE '50+ km'
    END
ORDER BY
    CASE
        WHEN distance_bucket = '0-5 km' THEN 1
        WHEN distance_bucket = '5-10 km' THEN 2
        WHEN distance_bucket = '10-20 km' THEN 3
        WHEN distance_bucket = '20-50 km' THEN 4
        ELSE 5
    END;


-- ============================================================
-- 7. Warehouse delivery performance
-- ============================================================

SELECT
    warehouse_id,
    COUNT(*) AS total_orders,
    ROUND(AVG(order_value), 2) AS avg_order_value,
    ROUND(AVG(delivery_distance_km), 2) AS avg_distance_km,
    ROUND(AVG(actual_delivery_time_min), 2) AS avg_actual_time_min,
    ROUND(
        AVG(
            CASE
                WHEN actual_delivery_time_min > promised_delivery_time_min
                THEN actual_delivery_time_min - promised_delivery_time_min
                ELSE 0
            END
        ),
        2
    ) AS avg_delay_min
FROM orders
GROUP BY warehouse_id
ORDER BY avg_delay_min DESC;


-- ============================================================
-- 8. Driver delivery performance
-- ============================================================

SELECT
    driver_id,
    COUNT(*) AS total_deliveries,
    ROUND(AVG(delivery_distance_km), 2) AS avg_distance_km,
    ROUND(AVG(actual_delivery_time_min), 2) AS avg_actual_time_min,
    SUM(
        CASE
            WHEN actual_delivery_time_min <= promised_delivery_time_min
            THEN 1
            ELSE 0
        END
    ) AS on_time_deliveries,
    ROUND(
        SUM(
            CASE
                WHEN actual_delivery_time_min <= promised_delivery_time_min
                THEN 1
                ELSE 0
            END
        ) * 100.0 / COUNT(*),
        2
    ) AS on_time_rate_pct
FROM orders
GROUP BY driver_id
ORDER BY on_time_rate_pct DESC;


-- ============================================================
-- 9. Daily delivery trend
-- ============================================================

SELECT
    order_date,
    COUNT(*) AS total_orders,
    SUM(
        CASE
            WHEN actual_delivery_time_min > promised_delivery_time_min
            THEN 1
            ELSE 0
        END
    ) AS delayed_orders,
    ROUND(
        AVG(actual_delivery_time_min),
        2
    ) AS avg_actual_time_min
FROM orders
GROUP BY order_date
ORDER BY order_date;


-- ============================================================
-- 10. Delivery revenue by region
-- ============================================================

SELECT
    region,
    COUNT(*) AS total_orders,
    ROUND(SUM(order_value), 2) AS total_order_value,
    ROUND(AVG(order_value), 2) AS avg_order_value
FROM orders
GROUP BY region
ORDER BY total_order_value DESC;


-- ============================================================
-- 11. Identify severe delays
-- ============================================================

SELECT
    order_id,
    customer_id,
    warehouse_id,
    driver_id,
    region,
    delivery_distance_km,
    promised_delivery_time_min,
    actual_delivery_time_min,
    actual_delivery_time_min - promised_delivery_time_min AS delay_min
FROM orders
WHERE actual_delivery_time_min - promised_delivery_time_min > 30
ORDER BY delay_min DESC;


-- ============================================================
-- 12. Delivery KPI summary
-- ============================================================

SELECT
    COUNT(*) AS total_orders,
    SUM(
        CASE
            WHEN actual_delivery_time_min <= promised_delivery_time_min
            THEN 1
            ELSE 0
        END
    ) AS on_time_orders,
    SUM(
        CASE
            WHEN actual_delivery_time_min > promised_delivery_time_min
            THEN 1
            ELSE 0
        END
    ) AS delayed_orders,
    ROUND(
        SUM(
            CASE
                WHEN actual_delivery_time_min <= promised_delivery_time_min
                THEN 1
                ELSE 0
            END
        ) * 100.0 / COUNT(*),
        2
    ) AS on_time_delivery_rate_pct,
    ROUND(AVG(order_value), 2) AS avg_order_value,
    ROUND(SUM(order_value), 2) AS total_order_value
FROM orders;
