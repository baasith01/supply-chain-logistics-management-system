-- ============================================================
-- Logistics & Supply Chain Intelligence
-- Dimension: Driver
-- ============================================================
-- Grain:
--   One row per unique driver.
--
-- Source:
--   data/raw/drivers.csv
--
-- Purpose:
--   Store descriptive driver attributes used by delivery facts,
--   performance analytics, dbt models, and Power BI.
--
-- Surrogate key:
--   driver_key
--
-- Natural/source key:
--   driver_id
-- ============================================================

CREATE TABLE IF NOT EXISTS logistics_warehouse.dim_driver (
    driver_key BIGINT PRIMARY KEY,
    driver_id VARCHAR(50) NOT NULL,
    driver_name VARCHAR(150),
    region VARCHAR(100),
    experience_years DECIMAL(6,2),
    rating DECIMAL(4,2),
    employment_status VARCHAR(50),
    license_type VARCHAR(50),

    source_system VARCHAR(50) DEFAULT 'synthetic_source',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_dim_driver_driver_id
        UNIQUE (driver_id)
);


-- ============================================================
-- Recommended source-to-dimension mapping
-- ============================================================
--
-- driver_id          -> driver_id
-- driver_name        -> driver_name
-- region             -> region
-- experience_years  -> experience_years
-- rating             -> rating
-- employment_status  -> employment_status
-- license_type       -> license_type
--
-- driver_key is the warehouse surrogate key.
-- ============================================================


-- ============================================================
-- Data quality checks
-- ============================================================

-- Check duplicate driver IDs:
-- SELECT driver_id, COUNT(*)
-- FROM logistics_warehouse.dim_driver
-- GROUP BY driver_id
-- HAVING COUNT(*) > 1;

-- Check missing driver IDs:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.dim_driver
-- WHERE driver_id IS NULL;

-- Check invalid experience:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.dim_driver
-- WHERE experience_years < 0;

-- Check rating range:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.dim_driver
-- WHERE rating < 0 OR rating > 5;

-- Review driver employment status:
-- SELECT employment_status, COUNT(*)
-- FROM logistics_warehouse.dim_driver
-- GROUP BY employment_status
-- ORDER BY COUNT(*) DESC;


-- ============================================================
-- Example analytical queries
-- ============================================================

-- Driver count by region:
-- SELECT
--     region,
--     COUNT(*) AS driver_count
-- FROM logistics_warehouse.dim_driver
-- GROUP BY region
-- ORDER BY driver_count DESC;


-- Average driver rating by region:
-- SELECT
--     region,
--     ROUND(AVG(rating), 2) AS avg_rating
-- FROM logistics_warehouse.dim_driver
-- GROUP BY region
-- ORDER BY avg_rating DESC;


-- Experience distribution:
-- SELECT
--     CASE
--         WHEN experience_years < 2 THEN '0-2 Years'
--         WHEN experience_years < 5 THEN '2-5 Years'
--         WHEN experience_years < 10 THEN '5-10 Years'
--         ELSE '10+ Years'
--     END AS experience_band,
--     COUNT(*) AS driver_count
-- FROM logistics_warehouse.dim_driver
-- GROUP BY
--     CASE
--         WHEN experience_years < 2 THEN '0-2 Years'
--         WHEN experience_years < 5 THEN '2-5 Years'
--         WHEN experience_years < 10 THEN '5-10 Years'
--         ELSE '10+ Years'
--     END
-- ORDER BY driver_count DESC;


-- ============================================================
-- Relationship
-- ============================================================
--
-- fact_orders.driver_key
--          |
--          v
-- dim_driver.driver_key
--
-- fact_delivery.driver_key
--          |
--          v
-- dim_driver.driver_key
--
-- fact_gps.driver_key
--          |
--          v
-- dim_driver.driver_key
--
-- This supports analysis by:
--   * driver
--   * region
--   * experience
--   * rating
--   * employment status
--   * license type
-- ============================================================
