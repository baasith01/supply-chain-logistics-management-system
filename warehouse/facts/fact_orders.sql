-- ============================================================
-- Logistics & Supply Chain Intelligence
-- Fact: Orders
-- ============================================================
-- Grain:
--   One row per unique order.
--
-- Source:
--   data/processed/orders_clean.csv
--   data/processed/delivery_features.csv
--
-- Purpose:
--   Store measurable order-level business events for revenue,
--   volume, delivery, customer, driver, vehicle, warehouse,
--   regional, and time-based analytics.
--
-- Fact type:
--   Transaction fact
--
-- Primary key:
--   order_key (warehouse surrogate key)
--
-- Degenerate/source business key:
--   order_id
-- ============================================================

CREATE TABLE IF NOT EXISTS logistics_warehouse.fact_orders (
    order_key BIGINT PRIMARY KEY,

    -- Source/business identifiers
    order_id VARCHAR(50) NOT NULL,

    -- Dimension foreign keys
    customer_key BIGINT,
    driver_key BIGINT,
    vehicle_key BIGINT,
    warehouse_key BIGINT,
    location_key BIGINT,
    date_key INTEGER,

    -- Order dimensions/statuses
    order_status VARCHAR(50),
    delivery_status VARCHAR(50),

    -- Measures
    order_value DECIMAL(14,2),
    delivery_distance_km DECIMAL(12,2),
    promised_delivery_time_min DECIMAL(12,2),
    actual_delivery_time_min DECIMAL(12,2),
    delivery_delay_min DECIMAL(12,2),
    is_delayed INTEGER,

    -- Audit metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_fact_orders_order_id
        UNIQUE (order_id),

    CONSTRAINT fk_fact_orders_customer
        FOREIGN KEY (customer_key)
        REFERENCES logistics_warehouse.dim_customer(customer_key),

    CONSTRAINT fk_fact_orders_driver
        FOREIGN KEY (driver_key)
        REFERENCES logistics_warehouse.dim_driver(driver_key),

    CONSTRAINT fk_fact_orders_vehicle
        FOREIGN KEY (vehicle_key)
        REFERENCES logistics_warehouse.dim_vehicle(vehicle_key),

    CONSTRAINT fk_fact_orders_warehouse
        FOREIGN KEY (warehouse_key)
        REFERENCES logistics_warehouse.dim_warehouse(warehouse_key),

    CONSTRAINT fk_fact_orders_location
        FOREIGN KEY (location_key)
        REFERENCES logistics_warehouse.dim_location(location_key),

    CONSTRAINT fk_fact_orders_date
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
-- order_status                -> order_status
-- delivery_status             -> delivery_status
-- order_value                 -> order_value
-- delivery_distance_km        -> delivery_distance_km
-- promised_delivery_time_min  -> promised_delivery_time_min
-- actual_delivery_time_min    -> actual_delivery_time_min
-- delivery_delay_min          -> delivery_delay_min
-- is_delayed                  -> is_delayed
--
-- Important:
--   Dimension keys should be resolved during warehouse loading.
--   Do not copy source IDs into foreign-key columns when a
--   surrogate-key relationship is required.
-- ============================================================


-- ============================================================
-- Business grain and additive/semi-additive measures
-- ============================================================
--
-- Grain:
--   One row = one order.
--
-- Additive measures:
--   order_value
--   delivery_distance_km
--   delivery_delay_min
--   is_delayed
--
-- Usually aggregated as averages:
--   promised_delivery_time_min
--   actual_delivery_time_min
--
-- Derived metrics should generally be calculated from the
-- underlying measures rather than stored redundantly, e.g.:
--
--   Average order value
--       SUM(order_value) / COUNT(order_id)
--
--   Delay rate
--       SUM(is_delayed) / COUNT(order_id)
--
-- ============================================================


-- ============================================================
-- Data quality checks
-- ============================================================

-- Duplicate orders:
-- SELECT
--     order_id,
--     COUNT(*) AS duplicate_count
-- FROM logistics_warehouse.fact_orders
-- GROUP BY order_id
-- HAVING COUNT(*) > 1;


-- Missing source order IDs:
-- SELECT COUNT(*) AS missing_order_ids
-- FROM logistics_warehouse.fact_orders
-- WHERE order_id IS NULL
--    OR TRIM(order_id) = '';


-- Invalid order values:
-- SELECT COUNT(*) AS invalid_order_values
-- FROM logistics_warehouse.fact_orders
-- WHERE order_value < 0;


-- Invalid delivery distance:
-- SELECT COUNT(*) AS invalid_distances
-- FROM logistics_warehouse.fact_orders
-- WHERE delivery_distance_km < 0;


-- Invalid promised/actual delivery times:
-- SELECT COUNT(*) AS invalid_delivery_times
-- FROM logistics_warehouse.fact_orders
-- WHERE promised_delivery_time_min < 0
--    OR actual_delivery_time_min < 0;


-- Validate delayed flag:
-- SELECT
--     is_delayed,
--     COUNT(*) AS order_count
-- FROM logistics_warehouse.fact_orders
-- GROUP BY is_delayed
-- ORDER BY is_delayed;


-- Validate delivery delay calculation:
-- SELECT COUNT(*) AS inconsistent_delay_rows
-- FROM logistics_warehouse.fact_orders
-- WHERE delivery_delay_min <>
--       actual_delivery_time_min - promised_delivery_time_min;


-- ============================================================
-- Example analytical queries
-- ============================================================

-- Total orders and revenue:
-- SELECT
--     COUNT(*) AS total_orders,
--     ROUND(SUM(order_value), 2) AS total_order_value
-- FROM logistics_warehouse.fact_orders;


-- Revenue by month:
-- SELECT
--     d.year_number,
--     d.month_number,
--     d.month_name,
--     COUNT(f.order_id) AS total_orders,
--     ROUND(SUM(f.order_value), 2) AS total_order_value
-- FROM logistics_warehouse.fact_orders f
-- JOIN logistics_warehouse.dim_date d
--     ON f.date_key = d.date_key
-- GROUP BY
--     d.year_number,
--     d.month_number,
--     d.month_name
-- ORDER BY
--     d.year_number,
--     d.month_number;


-- Revenue by customer type:
-- SELECT
--     c.customer_type,
--     COUNT(f.order_id) AS total_orders,
--     ROUND(SUM(f.order_value), 2) AS total_order_value
-- FROM logistics_warehouse.fact_orders f
-- JOIN logistics_warehouse.dim_customer c
--     ON f.customer_key = c.customer_key
-- GROUP BY c.customer_type
-- ORDER BY total_order_value DESC;


-- Orders by warehouse:
-- SELECT
--     w.warehouse_name,
--     w.region,
--     COUNT(f.order_id) AS total_orders,
--     ROUND(SUM(f.order_value), 2) AS total_order_value,
--     ROUND(AVG(f.delivery_delay_min), 2) AS avg_delay_min
-- FROM logistics_warehouse.fact_orders f
-- JOIN logistics_warehouse.dim_warehouse w
--     ON f.warehouse_key = w.warehouse_key
-- GROUP BY
--     w.warehouse_name,
--     w.region
-- ORDER BY total_orders DESC;


-- Driver order performance:
-- SELECT
--     dr.driver_name,
--     dr.region,
--     COUNT(f.order_id) AS total_orders,
--     SUM(f.is_delayed) AS delayed_orders,
--     ROUND(AVG(f.delivery_delay_min), 2) AS avg_delay_min
-- FROM logistics_warehouse.fact_orders f
-- JOIN logistics_warehouse.dim_driver dr
--     ON f.driver_key = dr.driver_key
-- GROUP BY
--     dr.driver_name,
--     dr.region
-- ORDER BY delayed_orders DESC;


-- Regional delivery performance:
-- SELECT
--     l.region,
--     COUNT(f.order_id) AS total_orders,
--     ROUND(SUM(f.order_value), 2) AS total_order_value,
--     ROUND(AVG(f.delivery_delay_min), 2) AS avg_delay_min,
--     ROUND(
--         100.0 * SUM(f.is_delayed) / COUNT(f.order_id),
--         2
--     ) AS delay_rate_pct
-- FROM logistics_warehouse.fact_orders f
-- JOIN logistics_warehouse.dim_location l
--     ON f.location_key = l.location_key
-- GROUP BY l.region
-- ORDER BY total_orders DESC;


-- ============================================================
-- Loading pattern
-- ============================================================
--
-- A typical warehouse loading process is:
--
-- 1. Read cleaned order data.
-- 2. Resolve source customer_id -> customer_key.
-- 3. Resolve source driver_id -> driver_key.
-- 4. Resolve source vehicle_id -> vehicle_key.
-- 5. Resolve source warehouse_id -> warehouse_key.
-- 6. Resolve location -> location_key.
-- 7. Convert order_date -> date_key.
-- 8. Insert the resulting transaction row.
--
-- Example conceptual query:
--
-- INSERT INTO logistics_warehouse.fact_orders (...)
-- SELECT
--     <generated_order_key>,
--     o.order_id,
--     c.customer_key,
--     dr.driver_key,
--     v.vehicle_key,
--     w.warehouse_key,
--     l.location_key,
--     d.date_key,
--     o.order_status,
--     o.delivery_status,
--     o.order_value,
--     o.delivery_distance_km,
--     o.promised_delivery_time_min,
--     o.actual_delivery_time_min,
--     o.delivery_delay_min,
--     o.is_delayed
-- FROM cleaned_orders o
-- LEFT JOIN dim_customer c
--     ON o.customer_id = c.customer_id
-- LEFT JOIN dim_driver dr
--     ON o.driver_id = dr.driver_id
-- LEFT JOIN dim_vehicle v
--     ON o.vehicle_id = v.vehicle_id
-- LEFT JOIN dim_warehouse w
--     ON o.warehouse_id = w.warehouse_id
-- LEFT JOIN dim_location l
--     ON o.region = l.region
-- LEFT JOIN dim_date d
--     ON o.order_date = d.full_date;
--
-- The exact surrogate-key generation mechanism depends on the
-- target warehouse.
-- ============================================================


-- ============================================================
-- Relationship summary
-- ============================================================
--
--                 dim_customer
--                      |
--                      |
-- dim_driver ---- fact_orders ---- dim_warehouse
--                      |
--                 dim_vehicle
--                      |
--                 dim_location
--                      |
--                   dim_date
--
-- fact_orders is the central transaction fact for:
--   * order volume
--   * revenue
--   * delivery distance
--   * promised vs actual delivery time
--   * delivery delays
--   * customer analysis
--   * driver analysis
--   * vehicle analysis
--   * warehouse analysis
--   * regional/time analysis
-- ============================================================
