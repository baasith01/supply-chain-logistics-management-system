-- Hive analytical queries: warehouse_analysis
-- Database: logistics_db
--
-- Purpose:
--   Analyze warehouse capacity, order volume, delivery performance,
--   geographic distribution, and operational workload.
--
-- Tables used:
--   warehouses
--   orders

USE logistics_db;

-- ============================================================
-- 1. Warehouse master summary
-- ============================================================

SELECT
    COUNT(*) AS total_warehouses,
    SUM(capacity_units) AS total_network_capacity_units,
    ROUND(AVG(capacity_units), 2) AS avg_warehouse_capacity_units,
    MIN(capacity_units) AS minimum_capacity_units,
    MAX(capacity_units) AS maximum_capacity_units
FROM warehouses;


-- ============================================================
-- 2. Warehouse capacity ranking
-- ============================================================

SELECT
    warehouse_id,
    warehouse_name,
    region,
    capacity_units
FROM warehouses
ORDER BY capacity_units DESC;


-- ============================================================
-- 3. Warehouse count and capacity by region
-- ============================================================

SELECT
    region,
    COUNT(*) AS warehouse_count,
    SUM(capacity_units) AS total_capacity_units,
    ROUND(AVG(capacity_units), 2) AS avg_capacity_units
FROM warehouses
GROUP BY region
ORDER BY total_capacity_units DESC;


-- ============================================================
-- 4. Order volume by warehouse
-- ============================================================

SELECT
    warehouse_id,
    COUNT(*) AS total_orders,
    ROUND(SUM(order_value), 2) AS total_order_value,
    ROUND(AVG(order_value), 2) AS avg_order_value,
    ROUND(AVG(delivery_distance_km), 2) AS avg_delivery_distance_km
FROM orders
GROUP BY warehouse_id
ORDER BY total_orders DESC;


-- ============================================================
-- 5. Warehouse delivery performance
-- ============================================================

SELECT
    warehouse_id,
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
    ) AS on_time_rate_pct,
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
ORDER BY on_time_rate_pct ASC;


-- ============================================================
-- 6. Warehouse capacity vs order workload
-- ============================================================
-- This compares the warehouse's configured capacity with observed
-- order volume. It is a workload indicator, not a true inventory
-- utilization calculation because inventory snapshots are unavailable.

WITH order_volume AS (
    SELECT
        warehouse_id,
        COUNT(*) AS total_orders,
        SUM(order_value) AS total_order_value
    FROM orders
    GROUP BY warehouse_id
)
SELECT
    w.warehouse_id,
    w.warehouse_name,
    w.region,
    w.capacity_units,
    COALESCE(o.total_orders, 0) AS total_orders,
    ROUND(
        COALESCE(o.total_orders, 0) * 100.0 / NULLIF(w.capacity_units, 0),
        2
    ) AS order_to_capacity_ratio_pct,
    ROUND(COALESCE(o.total_order_value, 0), 2) AS total_order_value
FROM warehouses w
LEFT JOIN order_volume o
    ON w.warehouse_id = o.warehouse_id
ORDER BY order_to_capacity_ratio_pct DESC;


-- ============================================================
-- 7. Warehouse region performance
-- ============================================================

SELECT
    o.region,
    COUNT(*) AS total_orders,
    COUNT(DISTINCT o.warehouse_id) AS active_warehouses,
    ROUND(SUM(o.order_value), 2) AS total_order_value,
    ROUND(AVG(o.delivery_distance_km), 2) AS avg_delivery_distance_km,
    ROUND(AVG(o.actual_delivery_time_min), 2) AS avg_delivery_time_min,
    ROUND(
        AVG(
            CASE
                WHEN o.actual_delivery_time_min > o.promised_delivery_time_min
                THEN o.actual_delivery_time_min - o.promised_delivery_time_min
                ELSE 0
            END
        ),
        2
    ) AS avg_delay_min
FROM orders o
GROUP BY o.region
ORDER BY total_orders DESC;


-- ============================================================
-- 8. Highest-delay warehouses
-- ============================================================

SELECT
    warehouse_id,
    COUNT(*) AS total_orders,
    ROUND(
        AVG(
            CASE
                WHEN actual_delivery_time_min > promised_delivery_time_min
                THEN actual_delivery_time_min - promised_delivery_time_min
                ELSE 0
            END
        ),
        2
    ) AS avg_delay_min,
    ROUND(
        SUM(
            CASE
                WHEN actual_delivery_time_min > promised_delivery_time_min
                THEN 1
                ELSE 0
            END
        ) * 100.0 / COUNT(*),
        2
    ) AS delay_rate_pct
FROM orders
GROUP BY warehouse_id
HAVING COUNT(*) >= 10
ORDER BY avg_delay_min DESC;


-- ============================================================
-- 9. Warehouse order-value contribution
-- ============================================================

SELECT
    warehouse_id,
    COUNT(*) AS total_orders,
    ROUND(SUM(order_value), 2) AS total_order_value,
    ROUND(
        SUM(order_value) * 100.0 /
        SUM(SUM(order_value)) OVER (),
        2
    ) AS revenue_contribution_pct
FROM orders
GROUP BY warehouse_id
ORDER BY total_order_value DESC;


-- ============================================================
-- 10. Warehouse geographic information
-- ============================================================

SELECT
    warehouse_id,
    warehouse_name,
    region,
    latitude,
    longitude,
    capacity_units
FROM warehouses
ORDER BY region, warehouse_name;


-- ============================================================
-- 11. Warehouse KPI summary
-- ============================================================

WITH warehouse_orders AS (
    SELECT
        warehouse_id,
        COUNT(*) AS total_orders,
        SUM(order_value) AS total_order_value,
        AVG(delivery_distance_km) AS avg_distance_km,
        AVG(actual_delivery_time_min) AS avg_delivery_time_min,
        AVG(
            CASE
                WHEN actual_delivery_time_min > promised_delivery_time_min
                THEN actual_delivery_time_min - promised_delivery_time_min
                ELSE 0
            END
        ) AS avg_delay_min,
        SUM(
            CASE
                WHEN actual_delivery_time_min <= promised_delivery_time_min
                THEN 1
                ELSE 0
            END
        ) * 100.0 / COUNT(*) AS on_time_rate_pct
    FROM orders
    GROUP BY warehouse_id
)
SELECT
    w.warehouse_id,
    w.warehouse_name,
    w.region,
    w.capacity_units,
    COALESCE(o.total_orders, 0) AS total_orders,
    ROUND(COALESCE(o.total_order_value, 0), 2) AS total_order_value,
    ROUND(COALESCE(o.avg_distance_km, 0), 2) AS avg_distance_km,
    ROUND(COALESCE(o.avg_delivery_time_min, 0), 2) AS avg_delivery_time_min,
    ROUND(COALESCE(o.avg_delay_min, 0), 2) AS avg_delay_min,
    ROUND(COALESCE(o.on_time_rate_pct, 0), 2) AS on_time_rate_pct
FROM warehouses w
LEFT JOIN warehouse_orders o
    ON w.warehouse_id = o.warehouse_id
ORDER BY total_orders DESC;
