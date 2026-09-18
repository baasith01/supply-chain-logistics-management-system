-- ============================================================
-- Logistics & Supply Chain Intelligence
-- Dimension: Location
-- ============================================================
-- Grain:
--   One row per unique analytical location.
--
-- Sources:
--   warehouses.csv
--   weather.csv
--   traffic.csv
--   GPS telemetry
--   Future maps/route APIs
--
-- Purpose:
--   Provide a conformed geographic dimension so orders, GPS,
--   traffic, weather, warehouses, and route analytics can be
--   analyzed consistently by location.
--
-- Surrogate key:
--   location_key
--
-- Natural/business key:
--   location_name
-- ============================================================

CREATE TABLE IF NOT EXISTS logistics_warehouse.dim_location (
    location_key BIGINT PRIMARY KEY,
    location_name VARCHAR(150) NOT NULL,
    region VARCHAR(100),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    location_type VARCHAR(50),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_dim_location_name
        UNIQUE (location_name)
);


-- ============================================================
-- Recommended source-to-dimension mapping
-- ============================================================
--
-- warehouse_name / location -> location_name
-- region                     -> region
-- latitude                   -> latitude
-- longitude                  -> longitude
--
-- location_type examples:
--   WAREHOUSE
--   REGION
--   TRAFFIC_POINT
--   WEATHER_POINT
--   GPS_AREA
--
-- A production implementation may use a separate geographic
-- hierarchy such as country -> state -> city -> zone -> point.
-- The current project keeps the model intentionally lightweight.
-- ============================================================


-- ============================================================
-- Data quality checks
-- ============================================================

-- Check duplicate locations:
-- SELECT location_name, COUNT(*)
-- FROM logistics_warehouse.dim_location
-- GROUP BY location_name
-- HAVING COUNT(*) > 1;

-- Check missing location names:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.dim_location
-- WHERE location_name IS NULL
--    OR TRIM(location_name) = '';

-- Check invalid latitude:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.dim_location
-- WHERE latitude IS NOT NULL
--   AND latitude NOT BETWEEN -90 AND 90;

-- Check invalid longitude:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.dim_location
-- WHERE longitude IS NOT NULL
--   AND longitude NOT BETWEEN -180 AND 180;

-- Review location types:
-- SELECT
--     location_type,
--     COUNT(*) AS location_count
-- FROM logistics_warehouse.dim_location
-- GROUP BY location_type
-- ORDER BY location_count DESC;


-- ============================================================
-- Example analytical queries
-- ============================================================

-- Locations by region:
-- SELECT
--     region,
--     COUNT(*) AS location_count
-- FROM logistics_warehouse.dim_location
-- GROUP BY region
-- ORDER BY location_count DESC;


-- Geographic location listing:
-- SELECT
--     location_key,
--     location_name,
--     region,
--     latitude,
--     longitude,
--     location_type
-- FROM logistics_warehouse.dim_location
-- ORDER BY region, location_name;


-- Locations with coordinates:
-- SELECT
--     location_name,
--     region,
--     latitude,
--     longitude
-- FROM logistics_warehouse.dim_location
-- WHERE latitude IS NOT NULL
--   AND longitude IS NOT NULL;


-- ============================================================
-- Relationship
-- ============================================================
--
-- fact_orders.location_key
--          |
--          v
-- dim_location.location_key
--
-- fact_delivery.location_key
--          |
--          v
-- dim_location.location_key
--
-- fact_gps.location_key
--          |
--          v
-- dim_location.location_key
--
-- fact_traffic.location_key
--          |
--          v
-- dim_location.location_key
--
-- The conformed location dimension enables:
--   * regional delivery analysis
--   * GPS movement analysis
--   * traffic hotspot analysis
--   * warehouse geographic analysis
--   * map-based Power BI visuals
--   * future weather/location joins
--
-- Important:
--   The current project should only populate a location_key when
--   the source-to-location mapping is reliable. It should not
--   fabricate geographic relationships between unrelated events.
-- ============================================================
