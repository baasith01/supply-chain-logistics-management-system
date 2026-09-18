-- ============================================================
-- Logistics & Supply Chain Intelligence
-- Dimension: Warehouse
-- ============================================================
-- Grain:
--   One row per unique warehouse.
--
-- Source:
--   data/raw/warehouses.csv
--
-- Purpose:
--   Store warehouse master data used by order, delivery, capacity,
--   regional, operational, dbt, and Power BI analytics.
--
-- Surrogate key:
--   warehouse_key
--
-- Natural/source key:
--   warehouse_id
--
-- Important modeling note:
--   The current project contains warehouse capacity and order activity,
--   but does not contain inventory snapshots. Therefore this dimension
--   does not claim to measure true inventory utilization.
-- ============================================================

CREATE TABLE IF NOT EXISTS logistics_warehouse.dim_warehouse (
    warehouse_key BIGINT PRIMARY KEY,
    warehouse_id VARCHAR(50) NOT NULL,
    warehouse_name VARCHAR(150),
    region VARCHAR(100),
    capacity_units DECIMAL(14,2),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),

    source_system VARCHAR(50) DEFAULT 'synthetic_source',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_dim_warehouse_warehouse_id
        UNIQUE (warehouse_id)
);


-- ============================================================
-- Recommended source-to-dimension mapping
-- ============================================================
--
-- warehouse_id    -> warehouse_id
-- warehouse_name  -> warehouse_name
-- region          -> region
-- capacity_units  -> capacity_units
-- latitude        -> latitude
-- longitude       -> longitude
--
-- warehouse_key is the warehouse surrogate key.
-- ============================================================


-- ============================================================
-- Data quality checks
-- ============================================================

-- Check duplicate warehouse IDs:
-- SELECT warehouse_id, COUNT(*)
-- FROM logistics_warehouse.dim_warehouse
-- GROUP BY warehouse_id
-- HAVING COUNT(*) > 1;

-- Check missing warehouse IDs:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.dim_warehouse
-- WHERE warehouse_id IS NULL;

-- Check invalid capacity:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.dim_warehouse
-- WHERE capacity_units <= 0;

-- Check invalid latitude:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.dim_warehouse
-- WHERE latitude NOT BETWEEN -90 AND 90;

-- Check invalid longitude:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.dim_warehouse
-- WHERE longitude NOT BETWEEN -180 AND 180;

-- Check missing regions:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.dim_warehouse
-- WHERE region IS NULL OR TRIM(region) = '';


-- ============================================================
-- Example analytical queries
-- ============================================================

-- Warehouse count by region:
-- SELECT
--     region,
--     COUNT(*) AS warehouse_count
-- FROM logistics_warehouse.dim_warehouse
-- GROUP BY region
-- ORDER BY warehouse_count DESC;


-- Capacity by region:
-- SELECT
--     region,
--     COUNT(*) AS warehouse_count,
--     ROUND(SUM(capacity_units), 2) AS total_capacity_units,
--     ROUND(AVG(capacity_units), 2) AS avg_capacity_units
-- FROM logistics_warehouse.dim_warehouse
-- GROUP BY region
-- ORDER BY total_capacity_units DESC;


-- Largest warehouses by capacity:
-- SELECT
--     warehouse_id,
--     warehouse_name,
--     region,
--     capacity_units
-- FROM logistics_warehouse.dim_warehouse
-- ORDER BY capacity_units DESC;


-- Geographic warehouse listing:
-- SELECT
--     warehouse_id,
--     warehouse_name,
--     region,
--     latitude,
--     longitude
-- FROM logistics_warehouse.dim_warehouse
-- ORDER BY region, warehouse_name;


-- ============================================================
-- Relationship
-- ============================================================
--
-- fact_orders.warehouse_key
--          |
--          v
-- dim_warehouse.warehouse_key
--
-- fact_delivery.warehouse_key
--          |
--          v
-- dim_warehouse.warehouse_key
--
-- The warehouse dimension supports:
--   * warehouse workload analysis
--   * regional order analysis
--   * delivery performance
--   * capacity planning
--   * geographic visualization
--   * executive Power BI dashboards
--
-- For current-project analytics, order volume can be compared
-- with capacity as a workload indicator. It must not be described
-- as actual inventory utilization without inventory data.
-- ============================================================
