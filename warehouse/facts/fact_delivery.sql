-- ============================================================
-- Logistics & Supply Chain Intelligence
-- Fact: Delivery
-- ============================================================
-- Grain:
--   One row per unique order-level delivery.
--
-- Source:
--   data/processed/delivery_features.csv
--   data/raw/delivery_updates.csv
--
-- Purpose:
--   Store delivery execution metrics separately from the broader
--   order transaction fact so delivery performance can be analyzed
--   independently.
--
-- Fact type:
--   Transaction / process-performance fact
--
-- Primary key:
--   delivery_key
--
-- Business key:
--   order_id
--
-- Important:
--   The current model treats the final order-level delivery state
--   as one delivery record. Event-level delivery_updates remain
--   suitable for streaming/event analytics and are not falsely
--   collapsed into multiple delivery facts here.
-- ============================================================

CREATE TABLE IF NOT EXISTS logistics_warehouse.fact_delivery (
    delivery_key BIGINT PRIMARY KEY,

    -- Business/source identifier
    order_id VARCHAR(50) NOT NULL,

    -- Dimension foreign keys
    customer_key BIGINT,
    driver_key BIGINT,
    vehicle_key BIGINT,
    warehouse_key BIGINT,
    location_key BIGINT,
    date_key INTEGER,

    -- Delivery attributes
    delivery_status VARCHAR(50),

    -- Delivery measures
    promised_delivery_time_min DECIMAL(12,2),
    actual_delivery_time_min DECIMAL(12,2),
    delivery_delay_min DECIMAL(12,2),
    delivery_distance_km DECIMAL(12,2),

    -- Derived performance flags
    is_delayed INTEGER,
    on_time_flag INTEGER,

    -- Audit metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_fact_delivery_order_id
        UNIQUE (order_id),

    CONSTRAINT fk_fact_delivery_customer
        FOREIGN KEY (customer_key)
        REFERENCES logistics_warehouse.dim_customer(customer_key),

    CONSTRAINT fk_fact_delivery_driver
        FOREIGN KEY (driver_key)
        REFERENCES logistics_warehouse.dim_driver(driver_key),

    CONSTRAINT fk_fact_delivery_vehicle
        FOREIGN KEY (vehicle_key)
        REFERENCES logistics_warehouse.dim_vehicle(vehicle_key),

    CONSTRAINT fk_fact_delivery_warehouse
        FOREIGN KEY (warehouse_key)
        REFERENCES logistics_warehouse.dim_warehouse(warehouse_key),

    CONSTRAINT fk_fact_delivery_location
        FOREIGN KEY (location_key)
        REFERENCES logistics_warehouse.dim_location(location_key),

    CONSTRAINT fk_fact_delivery_date
        FOREIGN KEY (date_key)
        REFERENCES logistics_warehouse.dim_date(date_key)
);


-- ============================================================
-- Recommended source-to-fact mapping
-- ============================================================
--
-- order_id                    -> order_id
-- customer_id                 -> dim_customer.customer_key
-- driver_id                   -> dim_driver.driver_key
-- vehicle_id                  -> dim_vehicle.vehicle_key
-- warehouse_id                -> dim_warehouse.warehouse_key
-- region                      -> dim_location.location_key
-- order_date                  -> dim_date.date_key
-- delivery_status             -> delivery_status
-- promised_delivery_time_min  -> promised_delivery_time_min
-- actual_delivery_time_min    -> actual_delivery_time_min
-- delivery_delay_min          -> delivery_delay_min
-- delivery_distance_km        -> delivery_distance_km
-- is_delayed                  -> is_delayed
--
-- on_time_flag should be derived from the delivery result:
--
--   1 = delivered on time
--   0 = delayed
--
-- A production implementation may use a richer definition that
-- considers SLA grace periods and cancelled/returned deliveries.
-- ============================================================


-- ============================================================
-- Business grain
-- ============================================================
--
-- One row = one order-level delivery.
--
-- Main measures:
--   promised_delivery_time_min
--   actual_delivery_time_min
--   delivery_delay_min
--   delivery_distance_km
--
-- Main performance flags:
--   is_delayed
--   on_time_flag
--
-- Common KPIs:
--   * delivery completion rate
--   * on-time delivery rate
--   * delay rate
--   * average delivery time
--   * average delay
--   * average delivery distance
--   * severe-delay count
-- ============================================================


-- ============================================================
-- Data quality checks
-- ============================================================

-- Duplicate delivery records:
-- SELECT
--     order_id,
--     COUNT(*) AS duplicate_count
-- FROM logistics_warehouse.fact_delivery
-- GROUP BY order_id
-- HAVING COUNT(*) > 1;


-- Missing order IDs:
-- SELECT COUNT(*) AS missing_order_ids
-- FROM logistics_warehouse.fact_delivery
-- WHERE order_id IS NULL
--    OR TRIM(order_id) = '';


-- Invalid delivery times:
-- SELECT COUNT(*) AS invalid_delivery_times
-- FROM logistics_warehouse.fact_delivery
-- WHERE promised_delivery_time_min < 0
--    OR actual_delivery_time_min < 0;


-- Invalid delivery distances:
-- SELECT COUNT(*) AS invalid_distances
-- FROM logistics_warehouse.fact_delivery
-- WHERE delivery_distance_km < 0;


-- Check delay calculation:
-- SELECT COUNT(*) AS inconsistent_delay_rows
-- FROM logistics_warehouse.fact_delivery
-- WHERE delivery_delay_min <>
--       actual_delivery_time_min - promised_delivery_time_min;


-- Validate flags:
-- SELECT
--     is_delayed,
--     on_time_flag,
--     COUNT(*) AS delivery_count
-- FROM logistics_warehouse.fact_delivery
-- GROUP BY
--     is_delayed,
--     on_time_flag
-- ORDER BY
--     is_delayed,
--     on_time_flag;


-- ============================================================
-- Example analytical queries
-- ============================================================

-- Overall delivery KPIs:
-- SELECT
--     COUNT(*) AS total_deliveries,
--     SUM(on_time_flag) AS on_time_deliveries,
--     SUM(is_delayed) AS delayed_deliveries,
--     ROUND(
--         100.0 * SUM(on_time_flag) / COUNT(*),
--         2
--     ) AS on_time_delivery_rate_pct,
--     ROUND(AVG(actual_delivery_time_min), 2)
--         AS avg_delivery_time_min,
--     ROUND(AVG(delivery_delay_min), 2)
--         AS avg_delay_min
-- FROM logistics_warehouse.fact_delivery;


-- Delivery performance by driver:
-- SELECT
--     dr.driver_name,
--     dr.region,
--     COUNT(f.order_id) AS total_deliveries,
--     SUM(f.on_time_flag) AS on_time_deliveries,
--     SUM(f.is_delayed) AS delayed_deliveries,
--     ROUND(
--         100.0 * SUM(f.on_time_flag) / COUNT(f.order_id),
--         2
--     ) AS on_time_rate_pct,
--     ROUND(AVG(f.delivery_delay_min), 2)
--         AS avg_delay_min
-- FROM logistics_warehouse.fact_delivery f
-- JOIN logistics_warehouse.dim_driver dr
--     ON f.driver_key = dr.driver_key
-- GROUP BY
--     dr.driver_name,
--     dr.region
-- ORDER BY
--     delayed_deliveries DESC;


-- Delivery performance by warehouse:
-- SELECT
--     w.warehouse_name,
--     w.region,
--     COUNT(f.order_id) AS total_deliveries,
--     ROUND(
--         100.0 * SUM(f.on_time_flag) / COUNT(f.order_id),
--         2
--     ) AS on_time_rate_pct,
--     ROUND(AVG(f.delivery_delay_min), 2)
--         AS avg_delay_min
-- FROM logistics_warehouse.fact_delivery f
-- JOIN logistics_warehouse.dim_warehouse w
--     ON f.warehouse_key = w.warehouse_key
-- GROUP BY
--     w.warehouse_name,
--     w.region
-- ORDER BY
--     on_time_rate_pct ASC;


-- Delivery performance over time:
-- SELECT
--     d.full_date,
--     COUNT(f.order_id) AS total_deliveries,
--     ROUND(
--         100.0 * SUM(f.on_time_flag) / COUNT(f.order_id),
--         2
--     ) AS on_time_rate_pct,
--     ROUND(AVG(f.delivery_delay_min), 2)
--         AS avg_delay_min
-- FROM logistics_warehouse.fact_delivery f
-- JOIN logistics_warehouse.dim_date d
--     ON f.date_key = d.date_key
-- GROUP BY d.full_date
-- ORDER BY d.full_date;


-- Distance-efficiency analysis:
-- SELECT
--     CASE
--         WHEN delivery_distance_km < 5 THEN '<5 km'
--         WHEN delivery_distance_km < 10 THEN '5-10 km'
--         WHEN delivery_distance_km < 25 THEN '10-25 km'
--         ELSE '25+ km'
--     END AS distance_band,
--     COUNT(*) AS deliveries,
--     ROUND(AVG(actual_delivery_time_min), 2)
--         AS avg_delivery_time_min,
--     ROUND(AVG(delivery_delay_min), 2)
--         AS avg_delay_min
-- FROM logistics_warehouse.fact_delivery
-- GROUP BY
--     CASE
--         WHEN delivery_distance_km < 5 THEN '<5 km'
--         WHEN delivery_distance_km < 10 THEN '5-10 km'
--         WHEN delivery_distance_km < 25 THEN '10-25 km'
--         ELSE '25+ km'
--     END
-- ORDER BY deliveries DESC;


-- Severe delays:
-- SELECT
--     order_id,
--     delivery_delay_min,
--     delivery_distance_km,
--     delivery_status
-- FROM logistics_warehouse.fact_delivery
-- WHERE delivery_delay_min >= 60
-- ORDER BY delivery_delay_min DESC;


-- ============================================================
-- Relationship summary
-- ============================================================
--
--                    dim_customer
--                         |
--                         |
-- dim_driver ---- fact_delivery ---- dim_warehouse
--                         |
--                    dim_vehicle
--                         |
--                    dim_location
--                         |
--                      dim_date
--
-- fact_delivery is optimized for:
--   * on-time delivery analysis
--   * delay analysis
--   * SLA monitoring
--   * driver performance
--   * warehouse performance
--   * regional delivery performance
--   * delivery-distance efficiency
--   * Power BI operational dashboards
-- ============================================================
