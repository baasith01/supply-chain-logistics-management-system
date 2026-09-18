-- Warehouse Analysis
-- Purpose:
--   Analyze warehouse workload, order throughput, delivery performance,
--   order value, regional workload, delay exposure, and operational risk.
--
-- Expected mart:
--   warehouse_mart
--
-- Important modeling note:
--   This analysis measures order workload and delivery performance.
--   It does NOT claim physical inventory utilization because the current
--   project dataset does not contain inventory movement, stock-on-hand,
--   inbound receipts, or warehouse capacity consumption transactions.
--
-- Assumptions:
--   - One row in warehouse_mart represents one warehouse.
--   - warehouse_id identifies the warehouse.
--   - total_orders represents order workload.
--   - total_order_value represents order value processed.
--   - average_delivery_delay_min represents downstream delivery delay.
--   - delayed_orders represents delayed deliveries.
--   - region identifies the warehouse operating region.
--
-- Adapt schema qualification to the target warehouse if required.


-- ============================================================
-- 1. Overall Warehouse KPIs
-- ============================================================

SELECT
    COUNT(DISTINCT warehouse_id) AS total_warehouses,
    SUM(total_orders) AS total_orders,
    ROUND(
        AVG(total_orders),
        2
    ) AS average_orders_per_warehouse,
    ROUND(
        SUM(total_order_value),
        2
    ) AS total_order_value,
    ROUND(
        AVG(average_delivery_delay_min),
        2
    ) AS average_delivery_delay_min,
    SUM(delayed_orders) AS total_delayed_orders,
    ROUND(
        SUM(delayed_orders) * 100.0 /
        NULLIF(SUM(total_orders), 0),
        2
    ) AS overall_delay_rate_pct
FROM warehouse_mart;


-- ============================================================
-- 2. Warehouse-Level Workload and Performance
-- ============================================================

SELECT
    warehouse_id,
    warehouse_name,
    region,
    total_orders,
    ROUND(total_order_value, 2) AS total_order_value,
    ROUND(average_order_value, 2) AS average_order_value,
    ROUND(average_delivery_delay_min, 2) AS average_delivery_delay_min,
    delayed_orders,
    ROUND(
        delayed_orders * 100.0 /
        NULLIF(total_orders, 0),
        2
    ) AS delay_rate_pct
FROM warehouse_mart
ORDER BY total_orders DESC;


-- ============================================================
-- 3. Top Warehouses by Order Volume
-- ============================================================

SELECT
    warehouse_id,
    warehouse_name,
    region,
    total_orders,
    ROUND(total_order_value, 2) AS total_order_value,
    ROUND(average_delivery_delay_min, 2) AS average_delivery_delay_min
FROM warehouse_mart
ORDER BY total_orders DESC
LIMIT 20;


-- ============================================================
-- 4. Top Warehouses by Order Value
-- ============================================================

SELECT
    warehouse_id,
    warehouse_name,
    region,
    total_orders,
    ROUND(total_order_value, 2) AS total_order_value,
    ROUND(
        total_order_value /
        NULLIF(total_orders, 0),
        2
    ) AS average_order_value
FROM warehouse_mart
ORDER BY total_order_value DESC
LIMIT 20;


-- ============================================================
-- 5. Warehouse Delivery Performance
-- ============================================================

SELECT
    warehouse_id,
    warehouse_name,
    total_orders,
    delayed_orders,
    ROUND(
        delayed_orders * 100.0 /
        NULLIF(total_orders, 0),
        2
    ) AS delay_rate_pct,
    ROUND(average_delivery_delay_min, 2) AS average_delivery_delay_min,
    CASE
        WHEN delayed_orders * 1.0 /
             NULLIF(total_orders, 0) >= 0.50
             OR average_delivery_delay_min >= 30
            THEN 'High Risk'
        WHEN delayed_orders * 1.0 /
             NULLIF(total_orders, 0) >= 0.25
             OR average_delivery_delay_min >= 15
            THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS warehouse_delivery_risk
FROM warehouse_mart
ORDER BY delay_rate_pct DESC, average_delivery_delay_min DESC;


-- ============================================================
-- 6. Warehouse Performance Segmentation
-- ============================================================

SELECT
    warehouse_id,
    warehouse_name,
    region,
    total_orders,
    ROUND(total_order_value, 2) AS total_order_value,
    ROUND(average_delivery_delay_min, 2) AS average_delivery_delay_min,
    CASE
        WHEN total_orders >= 750
             AND average_delivery_delay_min < 15
            THEN 'High Performing'
        WHEN total_orders >= 500
             AND average_delivery_delay_min < 25
            THEN 'Stable'
        WHEN average_delivery_delay_min < 35
            THEN 'Needs Improvement'
        ELSE 'High Risk'
    END AS performance_category
FROM warehouse_mart
ORDER BY
    CASE
        WHEN total_orders >= 750
             AND average_delivery_delay_min < 15 THEN 1
        WHEN total_orders >= 500
             AND average_delivery_delay_min < 25 THEN 2
        WHEN average_delivery_delay_min < 35 THEN 3
        ELSE 4
    END,
    total_orders DESC;


-- ============================================================
-- 7. Regional Warehouse Workload
-- ============================================================

SELECT
    region,
    COUNT(DISTINCT warehouse_id) AS warehouse_count,
    SUM(total_orders) AS total_orders,
    ROUND(
        SUM(total_orders) * 1.0 /
        NULLIF(COUNT(DISTINCT warehouse_id), 0),
        2
    ) AS average_orders_per_warehouse,
    ROUND(SUM(total_order_value), 2) AS total_order_value,
    ROUND(AVG(average_delivery_delay_min), 2) AS average_delay_min,
    SUM(delayed_orders) AS delayed_orders,
    ROUND(
        SUM(delayed_orders) * 100.0 /
        NULLIF(SUM(total_orders), 0),
        2
    ) AS delay_rate_pct
FROM warehouse_mart
GROUP BY region
ORDER BY total_orders DESC;


-- ============================================================
-- 8. Warehouse Revenue Contribution
-- ============================================================

SELECT
    warehouse_id,
    warehouse_name,
    region,
    total_orders,
    ROUND(total_order_value, 2) AS total_order_value,
    ROUND(
        total_order_value * 100.0 /
        NULLIF(
            (SELECT SUM(total_order_value) FROM warehouse_mart),
            0
        ),
        2
    ) AS value_contribution_pct
FROM warehouse_mart
ORDER BY total_order_value DESC;


-- ============================================================
-- 9. High-Workload Warehouses
--    Uses the average warehouse order volume as the baseline.
-- ============================================================

WITH warehouse_average AS (
    SELECT AVG(total_orders) AS average_orders
    FROM warehouse_mart
)
SELECT
    w.warehouse_id,
    w.warehouse_name,
    w.region,
    w.total_orders,
    ROUND(a.average_orders, 2) AS network_average_orders,
    ROUND(w.total_order_value, 2) AS total_order_value,
    ROUND(w.average_delivery_delay_min, 2) AS average_delay_min,
    ROUND(
        w.delayed_orders * 100.0 /
        NULLIF(w.total_orders, 0),
        2
    ) AS delay_rate_pct
FROM warehouse_mart w
CROSS JOIN warehouse_average a
WHERE w.total_orders > a.average_orders
ORDER BY w.total_orders DESC;


-- ============================================================
-- 10. High-Workload + Poor-Delivery Warehouses
-- ============================================================

WITH warehouse_average AS (
    SELECT AVG(total_orders) AS average_orders
    FROM warehouse_mart
)
SELECT
    w.warehouse_id,
    w.warehouse_name,
    w.region,
    w.total_orders,
    ROUND(a.average_orders, 2) AS network_average_orders,
    ROUND(w.average_delivery_delay_min, 2) AS average_delay_min,
    ROUND(
        w.delayed_orders * 100.0 /
        NULLIF(w.total_orders, 0),
        2
    ) AS delay_rate_pct
FROM warehouse_mart w
CROSS JOIN warehouse_average a
WHERE w.total_orders > a.average_orders
  AND (
        w.average_delivery_delay_min >= 20
        OR w.delayed_orders * 1.0 /
           NULLIF(w.total_orders, 0) >= 0.25
      )
ORDER BY w.total_orders DESC, delay_rate_pct DESC;


-- ============================================================
-- 11. Low-Workload Warehouses
-- ============================================================

WITH warehouse_average AS (
    SELECT AVG(total_orders) AS average_orders
    FROM warehouse_mart
)
SELECT
    w.warehouse_id,
    w.warehouse_name,
    w.region,
    w.total_orders,
    ROUND(a.average_orders, 2) AS network_average_orders,
    ROUND(w.total_order_value, 2) AS total_order_value,
    ROUND(w.average_delivery_delay_min, 2) AS average_delay_min
FROM warehouse_mart w
CROSS JOIN warehouse_average a
WHERE w.total_orders < a.average_orders
ORDER BY w.total_orders ASC;


-- ============================================================
-- 12. Warehouse Workload Concentration
--    NTILE(10) groups warehouses into workload deciles.
-- ============================================================

WITH ranked_warehouses AS (
    SELECT
        warehouse_id,
        warehouse_name,
        total_orders,
        NTILE(10) OVER (ORDER BY total_orders DESC) AS workload_decile
    FROM warehouse_mart
)
SELECT
    workload_decile,
    COUNT(*) AS warehouse_count,
    SUM(total_orders) AS total_orders,
    ROUND(AVG(total_orders), 2) AS average_orders
FROM ranked_warehouses
GROUP BY workload_decile
ORDER BY workload_decile;


-- ============================================================
-- 13. Warehouse Order Value Segmentation
-- ============================================================

SELECT
    warehouse_id,
    warehouse_name,
    region,
    total_orders,
    ROUND(total_order_value, 2) AS total_order_value,
    CASE
        WHEN total_order_value >= 100000 THEN 'High Value'
        WHEN total_order_value >= 50000 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS value_segment
FROM warehouse_mart
ORDER BY total_order_value DESC;


-- ============================================================
-- 14. Warehouse Average Order Value
-- ============================================================

SELECT
    warehouse_id,
    warehouse_name,
    region,
    total_orders,
    ROUND(
        total_order_value /
        NULLIF(total_orders, 0),
        2
    ) AS average_order_value,
    ROUND(total_order_value, 2) AS total_order_value
FROM warehouse_mart
ORDER BY average_order_value DESC;


-- ============================================================
-- 15. Warehouse Delay Impact
--    Estimates order value associated with delayed orders only
--    when delayed_order_value is available in the mart.
-- ============================================================

SELECT
    warehouse_id,
    warehouse_name,
    region,
    total_orders,
    delayed_orders,
    ROUND(
        delayed_orders * 100.0 /
        NULLIF(total_orders, 0),
        2
    ) AS delay_rate_pct,
    ROUND(average_delivery_delay_min, 2) AS average_delay_min
FROM warehouse_mart
ORDER BY delayed_orders DESC;


-- ============================================================
-- 16. Warehouse Ranking by Operational Performance
-- ============================================================

SELECT
    warehouse_id,
    warehouse_name,
    region,
    total_orders,
    ROUND(average_delivery_delay_min, 2) AS average_delay_min,
    ROUND(
        delayed_orders * 100.0 /
        NULLIF(total_orders, 0),
        2
    ) AS delay_rate_pct,
    DENSE_RANK() OVER (
        ORDER BY
            delayed_orders * 1.0 /
            NULLIF(total_orders, 0) ASC,
            average_delivery_delay_min ASC
    ) AS performance_rank
FROM warehouse_mart
ORDER BY performance_rank;


-- ============================================================
-- 17. Warehouse Exception List
-- ============================================================

SELECT
    warehouse_id,
    warehouse_name,
    region,
    total_orders,
    delayed_orders,
    ROUND(average_delivery_delay_min, 2) AS average_delay_min,
    ROUND(
        delayed_orders * 100.0 /
        NULLIF(total_orders, 0),
        2
    ) AS delay_rate_pct
FROM warehouse_mart
WHERE average_delivery_delay_min >= 30
   OR delayed_orders * 1.0 /
      NULLIF(total_orders, 0) >= 0.50
ORDER BY average_delay_min DESC, delay_rate_pct DESC;


-- ============================================================
-- 18. Warehouse Network Balance
--    Compares each warehouse's workload against the network mean.
-- ============================================================

WITH network_stats AS (
    SELECT
        AVG(total_orders) AS avg_orders,
        STDDEV(total_orders) AS stddev_orders
    FROM warehouse_mart
)
SELECT
    w.warehouse_id,
    w.warehouse_name,
    w.region,
    w.total_orders,
    ROUND(n.avg_orders, 2) AS network_avg_orders,
    ROUND(n.stddev_orders, 2) AS network_stddev_orders,
    CASE
        WHEN w.total_orders >
             n.avg_orders + COALESCE(n.stddev_orders, 0)
            THEN 'Above Normal Workload'
        WHEN w.total_orders <
             n.avg_orders - COALESCE(n.stddev_orders, 0)
            THEN 'Below Normal Workload'
        ELSE 'Within Normal Range'
    END AS workload_position
FROM warehouse_mart w
CROSS JOIN network_stats n
ORDER BY w.total_orders DESC;


-- ============================================================
-- 19. Warehouse Workload vs Delivery Delay
-- ============================================================

SELECT
    warehouse_id,
    warehouse_name,
    region,
    total_orders,
    ROUND(average_delivery_delay_min, 2) AS average_delay_min,
    CASE
        WHEN total_orders >=
             (SELECT AVG(total_orders) FROM warehouse_mart)
             AND average_delivery_delay_min >= 20
            THEN 'High Workload / High Delay'
        WHEN total_orders >=
             (SELECT AVG(total_orders) FROM warehouse_mart)
             AND average_delivery_delay_min < 20
            THEN 'High Workload / Controlled Delay'
        WHEN total_orders <
             (SELECT AVG(total_orders) FROM warehouse_mart)
             AND average_delivery_delay_min >= 20
            THEN 'Low Workload / High Delay'
        ELSE 'Low Workload / Controlled Delay'
    END AS workload_delay_segment
FROM warehouse_mart
ORDER BY total_orders DESC;


-- ============================================================
-- 20. Executive Warehouse Snapshot
-- ============================================================

SELECT
    COUNT(*) AS total_warehouses,
    SUM(total_orders) AS total_orders,
    ROUND(AVG(total_orders), 2) AS average_orders_per_warehouse,
    ROUND(SUM(total_order_value), 2) AS total_order_value,
    ROUND(AVG(total_order_value), 2) AS average_warehouse_order_value,
    ROUND(AVG(average_delivery_delay_min), 2) AS average_delivery_delay_min,
    SUM(delayed_orders) AS total_delayed_orders,
    ROUND(
        SUM(delayed_orders) * 100.0 /
        NULLIF(SUM(total_orders), 0),
        2
    ) AS network_delay_rate_pct,
    SUM(
        CASE
            WHEN average_delivery_delay_min >= 30
              OR delayed_orders * 1.0 /
                 NULLIF(total_orders, 0) >= 0.50
            THEN 1
            ELSE 0
        END
    ) AS high_risk_warehouses
FROM warehouse_mart;
