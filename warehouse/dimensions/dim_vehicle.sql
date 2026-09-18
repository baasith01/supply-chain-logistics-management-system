-- ============================================================
-- Logistics & Supply Chain Intelligence
-- Dimension: Vehicle
-- ============================================================
-- Grain:
--   One row per unique vehicle.
--
-- Source:
--   data/raw/vehicles.csv
--
-- Purpose:
--   Store descriptive fleet attributes used by order, delivery,
--   GPS, fleet-performance analytics, dbt models, and Power BI.
--
-- Surrogate key:
--   vehicle_key
--
-- Natural/source key:
--   vehicle_id
-- ============================================================

CREATE TABLE IF NOT EXISTS logistics_warehouse.dim_vehicle (
    vehicle_key BIGINT PRIMARY KEY,
    vehicle_id VARCHAR(50) NOT NULL,
    vehicle_type VARCHAR(50),
    driver_id VARCHAR(50),
    fuel_type VARCHAR(50),
    capacity_kg DECIMAL(12,2),
    manufacture_year INTEGER,
    maintenance_status VARCHAR(50),
    vehicle_status VARCHAR(50),
    odometer_km DECIMAL(14,2),

    source_system VARCHAR(50) DEFAULT 'synthetic_source',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_dim_vehicle_vehicle_id
        UNIQUE (vehicle_id)
);


-- ============================================================
-- Recommended source-to-dimension mapping
-- ============================================================
--
-- vehicle_id          -> vehicle_id
-- vehicle_type        -> vehicle_type
-- driver_id           -> driver_id
-- fuel_type           -> fuel_type
-- capacity_kg         -> capacity_kg
-- manufacture_year    -> manufacture_year
-- maintenance_status  -> maintenance_status
-- vehicle_status      -> vehicle_status
-- odometer_km         -> odometer_km
--
-- vehicle_key is the warehouse surrogate key.
-- ============================================================


-- ============================================================
-- Data quality checks
-- ============================================================

-- Check duplicate vehicle IDs:
-- SELECT vehicle_id, COUNT(*)
-- FROM logistics_warehouse.dim_vehicle
-- GROUP BY vehicle_id
-- HAVING COUNT(*) > 1;

-- Check missing vehicle IDs:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.dim_vehicle
-- WHERE vehicle_id IS NULL;

-- Check invalid capacity:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.dim_vehicle
-- WHERE capacity_kg <= 0;

-- Check invalid odometer:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.dim_vehicle
-- WHERE odometer_km < 0;

-- Check suspicious manufacture years:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.dim_vehicle
-- WHERE manufacture_year < 1900
--    OR manufacture_year > EXTRACT(YEAR FROM CURRENT_DATE) + 1;

-- Review maintenance status:
-- SELECT maintenance_status, COUNT(*)
-- FROM logistics_warehouse.dim_vehicle
-- GROUP BY maintenance_status
-- ORDER BY COUNT(*) DESC;

-- Review vehicle status:
-- SELECT vehicle_status, COUNT(*)
-- FROM logistics_warehouse.dim_vehicle
-- GROUP BY vehicle_status
-- ORDER BY COUNT(*) DESC;


-- ============================================================
-- Example analytical queries
-- ============================================================

-- Fleet count by vehicle type:
-- SELECT
--     vehicle_type,
--     COUNT(*) AS vehicle_count
-- FROM logistics_warehouse.dim_vehicle
-- GROUP BY vehicle_type
-- ORDER BY vehicle_count DESC;


-- Fleet count by fuel type:
-- SELECT
--     fuel_type,
--     COUNT(*) AS vehicle_count
-- FROM logistics_warehouse.dim_vehicle
-- GROUP BY fuel_type
-- ORDER BY vehicle_count DESC;


-- Maintenance status distribution:
-- SELECT
--     maintenance_status,
--     COUNT(*) AS vehicle_count
-- FROM logistics_warehouse.dim_vehicle
-- GROUP BY maintenance_status
-- ORDER BY vehicle_count DESC;


-- Vehicle age analysis:
-- SELECT
--     vehicle_type,
--     ROUND(
--         AVG(
--             EXTRACT(YEAR FROM CURRENT_DATE) - manufacture_year
--         ),
--         2
--     ) AS avg_vehicle_age_years
-- FROM logistics_warehouse.dim_vehicle
-- GROUP BY vehicle_type
-- ORDER BY avg_vehicle_age_years DESC;


-- Capacity by vehicle type:
-- SELECT
--     vehicle_type,
--     COUNT(*) AS vehicle_count,
--     ROUND(SUM(capacity_kg), 2) AS total_capacity_kg,
--     ROUND(AVG(capacity_kg), 2) AS avg_capacity_kg
-- FROM logistics_warehouse.dim_vehicle
-- GROUP BY vehicle_type
-- ORDER BY total_capacity_kg DESC;


-- ============================================================
-- Relationship
-- ============================================================
--
-- fact_orders.vehicle_key
--          |
--          v
-- dim_vehicle.vehicle_key
--
-- fact_delivery.vehicle_key
--          |
--          v
-- dim_vehicle.vehicle_key
--
-- fact_gps.vehicle_key
--          |
--          v
-- dim_vehicle.vehicle_key
--
-- The source driver_id is retained because it exists in the
-- source vehicle master. Analytical fact relationships should
-- preferably use driver_key from dim_driver when driver history
-- or slowly changing dimensions are introduced.
--
-- This dimension supports:
--   * fleet composition
--   * vehicle capacity analysis
--   * maintenance monitoring
--   * vehicle status analysis
--   * vehicle age analysis
--   * GPS/fleet performance
-- ============================================================
