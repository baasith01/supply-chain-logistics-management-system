-- Driver Analysis
-- Purpose:
--   Analyze driver productivity, delivery performance, reliability,
--   delay behavior, workload, and operational risk.
--
-- Expected marts:
--   delivery_mart
--   driver_metrics (optional processed output for additional operational analysis)
--
-- Assumptions:
--   - One row in delivery_mart represents one order.
--   - driver_id identifies the assigned driver.
--   - order_date is the order date.
--   - delivery_delay_min is the delivery delay in minutes.
--   - is_delayed is 1/0.
--   - order_value is the order value.
--   - delivery_distance_km is the delivery distance.
--   - region identifies the operating region.
--
-- Adapt schema qualification to the target warehouse if required.


-- ============================================================
-- 1. Overall Driver KPIs
-- ============================================================

SELECT
    COUNT(DISTINCT driver_id) AS active_drivers,
    COUNT(*) AS total_orders,
    ROUND(
        COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT driver_id), 0),
        2
    ) AS orders_per_driver,
    ROUND(AVG(delivery_delay_min), 2) AS average_delivery_delay_min,
    ROUND(
        AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) * 100,
        2
    ) AS overall_delay_rate_pct,
    ROUND(SUM(order_value), 2) AS total_order_value
FROM delivery_mart;


-- ============================================================
-- 2. Driver-Level Performance
-- ============================================================

SELECT
    driver_id,
    COUNT(*) AS total_orders,
    ROUND(SUM(order_value), 2) AS total_order_value,
    ROUND(AVG(order_value), 2) AS average_order_value,
    ROUND(AVG(delivery_distance_km), 2) AS average_distance_km,
    ROUND(AVG(delivery_delay_min), 2) AS average_delay_min,
    SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) AS delayed_orders,
    ROUND(
        AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) * 100,
        2
    ) AS delay_rate_pct,
    ROUND(
        AVG(CASE WHEN is_delayed = 0 THEN 1.0 ELSE 0.0 END) * 100,
        2
    ) AS on_time_rate_pct
FROM delivery_mart
GROUP BY driver_id
ORDER BY total_orders DESC;


-- ============================================================
-- 3. Top Drivers by Order Volume
-- ============================================================

SELECT
    driver_id,
    COUNT(*) AS total_orders,
    ROUND(SUM(order_value), 2) AS total_order_value,
    ROUND(AVG(delivery_delay_min), 2) AS average_delay_min,
    ROUND(
        AVG(CASE WHEN is_delayed = 0 THEN 1.0 ELSE 0.0 END) * 100,
        2
    ) AS on_time_rate_pct
FROM delivery_mart
GROUP BY driver_id
ORDER BY total_orders DESC
LIMIT 20;


-- ============================================================
-- 4. Top Drivers by Order Value
-- ============================================================

SELECT
    driver_id,
    COUNT(*) AS total_orders,
    ROUND(SUM(order_value), 2) AS total_order_value,
    ROUND(AVG(order_value), 2) AS average_order_value,
    ROUND(AVG(delivery_delay_min), 2) AS average_delay_min
FROM delivery_mart
GROUP BY driver_id
ORDER BY total_order_value DESC
LIMIT 20;


-- ============================================================
-- 5. Best Drivers by On-Time Performance
--    Minimum 10 orders avoids ranking very low-volume drivers.
-- ============================================================

SELECT
    driver_id,
    COUNT(*) AS total_orders,
    ROUND(
        AVG(CASE WHEN is_delayed = 0 THEN 1.0 ELSE 0.0 END) * 100,
        2
    ) AS on_time_rate_pct,
    ROUND(AVG(delivery_delay_min), 2) AS average_delay_min,
    SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) AS delayed_orders
FROM delivery_mart
GROUP BY driver_id
HAVING COUNT(*) >= 10
ORDER BY on_time_rate_pct DESC, average_delay_min ASC
LIMIT 20;


-- ============================================================
-- 6. Drivers with Repeated Delivery Delays
-- ============================================================

SELECT
    driver_id,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) AS delayed_orders,
    ROUND(
        AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) * 100,
        2
    ) AS delay_rate_pct,
    ROUND(AVG(delivery_delay_min), 2) AS average_delay_min,
    MAX(delivery_delay_min) AS maximum_delay_min
FROM delivery_mart
GROUP BY driver_id
HAVING SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) >= 3
ORDER BY delayed_orders DESC, average_delay_min DESC;


-- ============================================================
-- 7. Driver Performance Segmentation
-- ============================================================

WITH driver_metrics AS (
    SELECT
        driver_id,
        COUNT(*) AS total_orders,
        AVG(delivery_delay_min) AS average_delay_min,
        AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) AS delay_rate
    FROM delivery_mart
    GROUP BY driver_id
)
SELECT
    driver_id,
    total_orders,
    ROUND(average_delay_min, 2) AS average_delay_min,
    ROUND(delay_rate * 100, 2) AS delay_rate_pct,
    CASE
        WHEN total_orders >= 10 AND delay_rate <= 0.10
             AND average_delay_min < 10
            THEN 'High Performer'
        WHEN delay_rate <= 0.25
             AND average_delay_min < 20
            THEN 'Good Performer'
        WHEN delay_rate <= 0.50
             AND average_delay_min < 30
            THEN 'Needs Improvement'
        ELSE 'High Risk'
    END AS performance_category
FROM driver_metrics
ORDER BY
    CASE
        WHEN total_orders >= 10 AND delay_rate <= 0.10
             AND average_delay_min < 10 THEN 1
        WHEN delay_rate <= 0.25
             AND average_delay_min < 20 THEN 2
        WHEN delay_rate <= 0.50
             AND average_delay_min < 30 THEN 3
        ELSE 4
    END,
    delay_rate ASC;


-- ============================================================
-- 8. Driver Workload by Region
-- ============================================================

SELECT
    region,
    COUNT(DISTINCT driver_id) AS active_drivers,
    COUNT(*) AS total_orders,
    ROUND(
        COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT driver_id), 0),
        2
    ) AS orders_per_driver,
    ROUND(AVG(delivery_delay_min), 2) AS average_delay_min,
    ROUND(
        AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) * 100,
        2
    ) AS delay_rate_pct
FROM delivery_mart
GROUP BY region
ORDER BY orders_per_driver DESC;


-- ============================================================
-- 9. Driver Performance by Region
-- ============================================================

SELECT
    region,
    driver_id,
    COUNT(*) AS total_orders,
    ROUND(AVG(delivery_delay_min), 2) AS average_delay_min,
    ROUND(
        AVG(CASE WHEN is_delayed = 0 THEN 1.0 ELSE 0.0 END) * 100,
        2
    ) AS on_time_rate_pct,
    ROUND(SUM(order_value), 2) AS total_order_value
FROM delivery_mart
GROUP BY region, driver_id
ORDER BY region, on_time_rate_pct DESC;


-- ============================================================
-- 10. Driver Revenue Contribution
-- ============================================================

SELECT
    driver_id,
    COUNT(*) AS total_orders,
    ROUND(SUM(order_value), 2) AS total_order_value,
    ROUND(
        SUM(order_value) * 100.0 /
        NULLIF((SELECT SUM(order_value) FROM delivery_mart), 0),
        2
    ) AS revenue_contribution_pct
FROM delivery_mart
GROUP BY driver_id
ORDER BY total_order_value DESC
LIMIT 20;


-- ============================================================
-- 11. Driver Distance and Delivery Efficiency
-- ============================================================

SELECT
    driver_id,
    COUNT(*) AS total_orders,
    ROUND(SUM(delivery_distance_km), 2) AS total_distance_km,
    ROUND(AVG(delivery_distance_km), 2) AS average_distance_km,
    ROUND(SUM(order_value), 2) AS total_order_value,
    ROUND(
        SUM(order_value) /
        NULLIF(SUM(delivery_distance_km), 0),
        2
    ) AS order_value_per_km,
    ROUND(
        AVG(delivery_distance_km) /
        NULLIF(COUNT(*), 0),
        2
    ) AS distance_per_order_km
FROM delivery_mart
GROUP BY driver_id
ORDER BY order_value_per_km DESC;


-- ============================================================
-- 12. Driver Performance by Delivery Distance
-- ============================================================

SELECT
    driver_id,
    CASE
        WHEN delivery_distance_km < 5 THEN 'Short Distance'
        WHEN delivery_distance_km < 15 THEN 'Medium Distance'
        WHEN delivery_distance_km < 30 THEN 'Long Distance'
        ELSE 'Very Long Distance'
    END AS distance_segment,
    COUNT(*) AS total_orders,
    ROUND(AVG(delivery_delay_min), 2) AS average_delay_min,
    ROUND(
        AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) * 100,
        2
    ) AS delay_rate_pct
FROM delivery_mart
GROUP BY
    driver_id,
    CASE
        WHEN delivery_distance_km < 5 THEN 'Short Distance'
        WHEN delivery_distance_km < 15 THEN 'Medium Distance'
        WHEN delivery_distance_km < 30 THEN 'Long Distance'
        ELSE 'Very Long Distance'
    END
ORDER BY driver_id, average_delay_min DESC;


-- ============================================================
-- 13. Driver Performance During Rainy Conditions
-- ============================================================

SELECT
    driver_id,
    COUNT(*) AS total_orders,
    ROUND(
        AVG(CASE WHEN is_rainy = 1 THEN 1.0 ELSE 0.0 END) * 100,
        2
    ) AS rainy_order_share_pct,
    ROUND(
        AVG(
            CASE
                WHEN is_rainy = 1 AND is_delayed = 1
                THEN 1.0
                ELSE 0.0
            END
        ) * 100,
        2
    ) AS rainy_delay_rate_pct,
    ROUND(
        AVG(
            CASE
                WHEN is_rainy = 0 AND is_delayed = 1
                THEN 1.0
                ELSE 0.0
            END
        ) * 100,
        2
    ) AS non_rainy_delay_rate_pct,
    ROUND(AVG(delivery_delay_min), 2) AS average_delay_min
FROM delivery_mart
GROUP BY driver_id
ORDER BY rainy_delay_rate_pct DESC;


-- ============================================================
-- 14. Driver Performance by Weather Risk
-- ============================================================

SELECT
    driver_id,
    weather_risk,
    COUNT(*) AS total_orders,
    ROUND(AVG(delivery_delay_min), 2) AS average_delay_min,
    ROUND(
        AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) * 100,
        2
    ) AS delay_rate_pct
FROM delivery_mart
GROUP BY driver_id, weather_risk
ORDER BY driver_id, delay_rate_pct DESC;


-- ============================================================
-- 15. Driver Daily Performance Trend
-- ============================================================

SELECT
    order_date,
    driver_id,
    COUNT(*) AS total_orders,
    ROUND(AVG(delivery_delay_min), 2) AS average_delay_min,
    SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) AS delayed_orders,
    ROUND(
        AVG(CASE WHEN is_delayed = 0 THEN 1.0 ELSE 0.0 END) * 100,
        2
    ) AS on_time_rate_pct,
    ROUND(SUM(order_value), 2) AS total_order_value
FROM delivery_mart
GROUP BY order_date, driver_id
ORDER BY order_date, driver_id;


-- ============================================================
-- 16. Driver Monthly Performance
-- ============================================================

SELECT
    EXTRACT(YEAR FROM order_date) AS order_year,
    EXTRACT(MONTH FROM order_date) AS order_month,
    driver_id,
    COUNT(*) AS total_orders,
    ROUND(SUM(order_value), 2) AS total_order_value,
    ROUND(AVG(delivery_delay_min), 2) AS average_delay_min,
    ROUND(
        AVG(CASE WHEN is_delayed = 0 THEN 1.0 ELSE 0.0 END) * 100,
        2
    ) AS on_time_rate_pct
FROM delivery_mart
GROUP BY
    EXTRACT(YEAR FROM order_date),
    EXTRACT(MONTH FROM order_date),
    driver_id
ORDER BY order_year, order_month, total_orders DESC;


-- ============================================================
-- 17. Driver Workload Concentration
--    NTILE(10) creates driver workload deciles.
-- ============================================================

WITH driver_orders AS (
    SELECT
        driver_id,
        COUNT(*) AS total_orders
    FROM delivery_mart
    GROUP BY driver_id
),
ranked_drivers AS (
    SELECT
        driver_id,
        total_orders,
        NTILE(10) OVER (ORDER BY total_orders DESC) AS workload_decile
    FROM driver_orders
)
SELECT
    workload_decile,
    COUNT(*) AS driver_count,
    SUM(total_orders) AS total_orders,
    ROUND(AVG(total_orders), 2) AS average_orders_per_driver
FROM ranked_drivers
GROUP BY workload_decile
ORDER BY workload_decile;


-- ============================================================
-- 18. High Workload + Poor Performance
-- ============================================================

WITH driver_metrics AS (
    SELECT
        driver_id,
        COUNT(*) AS total_orders,
        AVG(delivery_delay_min) AS average_delay_min,
        AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) AS delay_rate
    FROM delivery_mart
    GROUP BY driver_id
),
workload_threshold AS (
    SELECT
        AVG(total_orders) AS average_driver_orders
    FROM driver_metrics
)
SELECT
    d.driver_id,
    d.total_orders,
    ROUND(d.average_delay_min, 2) AS average_delay_min,
    ROUND(d.delay_rate * 100, 2) AS delay_rate_pct
FROM driver_metrics d
CROSS JOIN workload_threshold w
WHERE d.total_orders > w.average_driver_orders
  AND (d.delay_rate >= 0.25 OR d.average_delay_min >= 20)
ORDER BY d.total_orders DESC, d.delay_rate DESC;


-- ============================================================
-- 19. Driver Exception List
-- ============================================================

SELECT
    driver_id,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) AS delayed_orders,
    MAX(delivery_delay_min) AS maximum_delay_min,
    ROUND(AVG(delivery_delay_min), 2) AS average_delay_min,
    ROUND(SUM(order_value), 2) AS total_order_value
FROM delivery_mart
GROUP BY driver_id
HAVING
    AVG(delivery_delay_min) >= 30
    OR SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) >= 5
ORDER BY average_delay_min DESC;


-- ============================================================
-- 20. Executive Driver Snapshot
-- ============================================================

WITH driver_metrics AS (
    SELECT
        driver_id,
        COUNT(*) AS total_orders,
        SUM(order_value) AS total_order_value,
        AVG(delivery_delay_min) AS average_delay_min,
        AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) AS delay_rate
    FROM delivery_mart
    GROUP BY driver_id
)
SELECT
    COUNT(*) AS total_drivers,
    SUM(CASE WHEN total_orders > 0 THEN 1 ELSE 0 END) AS active_drivers,
    SUM(CASE WHEN delay_rate <= 0.10 THEN 1 ELSE 0 END) AS high_reliability_drivers,
    SUM(CASE WHEN delay_rate >= 0.50 THEN 1 ELSE 0 END) AS high_risk_drivers,
    ROUND(AVG(total_orders), 2) AS average_orders_per_driver,
    ROUND(AVG(total_order_value), 2) AS average_driver_order_value,
    ROUND(AVG(average_delay_min), 2) AS average_driver_delay_min,
    ROUND(AVG(delay_rate) * 100, 2) AS average_driver_delay_rate_pct
FROM driver_metrics;
