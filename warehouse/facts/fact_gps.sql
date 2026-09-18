-- ============================================================
-- Logistics & Supply Chain Intelligence
-- Fact: GPS
-- ============================================================
-- Grain:
--   One row per vehicle GPS observation.
--
-- Source:
--   data/processed/gps_clean.csv
--
-- Purpose:
--   Store vehicle telemetry events for fleet monitoring, movement
--   analysis, route intelligence, signal-quality monitoring, and
--   operational dashboards.
--
-- Fact type:
--   Event / telemetry fact
--
-- Primary key:
--   gps_key
--
-- Natural event identifiers:
--   vehicle_id + event_timestamp
--
-- Important:
--   The current GPS dataset does not provide an event_id. The
--   combination of vehicle_id and timestamp is therefore treated
--   as the natural event identity after duplicate cleaning.
-- ============================================================

CREATE TABLE IF NOT EXISTS logistics_warehouse.fact_gps (
    gps_key BIGINT PRIMARY KEY,

    -- Dimension foreign keys
    vehicle_key BIGINT,
    driver_key BIGINT,
    location_key BIGINT,
    date_key INTEGER,

    -- Source identifiers
    vehicle_id VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,

    -- Geographic telemetry
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),

    -- Movement telemetry
    speed_kmph DECIMAL(12,2),
    heading DECIMAL(8,2),

    -- Vehicle state
    vehicle_status VARCHAR(50),
    ignition_status VARCHAR(50),
    signal_quality VARCHAR(50),

    -- Derived telemetry flags
    is_moving INTEGER,
    is_low_signal INTEGER,

    -- Audit metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_fact_gps_vehicle
        FOREIGN KEY (vehicle_key)
        REFERENCES logistics_warehouse.dim_vehicle(vehicle_key),

    CONSTRAINT fk_fact_gps_driver
        FOREIGN KEY (driver_key)
        REFERENCES logistics_warehouse.dim_driver(driver_key),

    CONSTRAINT fk_fact_gps_location
        FOREIGN KEY (location_key)
        REFERENCES logistics_warehouse.dim_location(location_key),

    CONSTRAINT fk_fact_gps_date
        FOREIGN KEY (date_key)
        REFERENCES logistics_warehouse.dim_date(date_key)
);


-- ============================================================
-- Recommended source-to-fact mapping
-- ============================================================
--
-- vehicle_id       -> vehicle_id + dim_vehicle.vehicle_key
-- timestamp        -> event_timestamp
-- region           -> dim_location.location_key
-- latitude         -> latitude
-- longitude        -> longitude
-- speed_kmph       -> speed_kmph
-- heading          -> heading
-- vehicle_status   -> vehicle_status
-- ignition_status  -> ignition_status
-- signal_quality   -> signal_quality
-- is_moving        -> is_moving
-- is_low_signal    -> is_low_signal
-- timestamp/date   -> dim_date.date_key
--
-- driver_key can be resolved from the vehicle/driver relationship
-- when the driver assignment is known and valid.
--
-- Important:
--   Do not fabricate a GPS-to-order relationship. The current
--   source data does not contain an order_id for each GPS event.
-- ============================================================


-- ============================================================
-- Business grain
-- ============================================================
--
-- One row = one vehicle telemetry observation.
--
-- Core measures:
--   speed_kmph
--   latitude
--   longitude
--   heading
--
-- Operational flags:
--   is_moving
--   is_low_signal
--
-- Common KPIs:
--   * average vehicle speed
--   * moving-event rate
--   * low-signal rate
--   * tracked vehicle count
--   * maximum observed speed
--   * vehicle activity by hour/day/region
-- ============================================================


-- ============================================================
-- Data quality checks
-- ============================================================

-- Duplicate GPS observations:
-- SELECT
--     vehicle_id,
--     event_timestamp,
--     COUNT(*) AS duplicate_count
-- FROM logistics_warehouse.fact_gps
-- GROUP BY
--     vehicle_id,
--     event_timestamp
-- HAVING COUNT(*) > 1;


-- Missing vehicle IDs:
-- SELECT COUNT(*) AS missing_vehicle_ids
-- FROM logistics_warehouse.fact_gps
-- WHERE vehicle_id IS NULL
--    OR TRIM(vehicle_id) = '';


-- Missing timestamps:
-- SELECT COUNT(*) AS missing_timestamps
-- FROM logistics_warehouse.fact_gps
-- WHERE event_timestamp IS NULL;


-- Invalid latitude:
-- SELECT COUNT(*) AS invalid_latitude
-- FROM logistics_warehouse.fact_gps
-- WHERE latitude NOT BETWEEN -90 AND 90;


-- Invalid longitude:
-- SELECT COUNT(*) AS invalid_longitude
-- FROM logistics_warehouse.fact_gps
-- WHERE longitude NOT BETWEEN -180 AND 180;


-- Invalid speed:
-- SELECT COUNT(*) AS invalid_speed
-- FROM logistics_warehouse.fact_gps
-- WHERE speed_kmph < 0;


-- Invalid heading:
-- SELECT COUNT(*) AS invalid_heading
-- FROM logistics_warehouse.fact_gps
-- WHERE heading < 0
--    OR heading >= 360;


-- Validate movement flag:
-- SELECT
--     is_moving,
--     COUNT(*) AS event_count
-- FROM logistics_warehouse.fact_gps
-- GROUP BY is_moving
-- ORDER BY is_moving;


-- Validate signal flag:
-- SELECT
--     is_low_signal,
--     COUNT(*) AS event_count
-- FROM logistics_warehouse.fact_gps
-- GROUP BY is_low_signal
-- ORDER BY is_low_signal;


-- ============================================================
-- Example analytical queries
-- ============================================================

-- Overall fleet telemetry:
-- SELECT
--     COUNT(*) AS gps_events,
--     COUNT(DISTINCT vehicle_id) AS tracked_vehicles,
--     ROUND(AVG(speed_kmph), 2) AS avg_speed_kmph,
--     ROUND(MAX(speed_kmph), 2) AS max_speed_kmph
-- FROM logistics_warehouse.fact_gps;


-- Vehicle movement performance:
-- SELECT
--     v.vehicle_id,
--     v.vehicle_type,
--     COUNT(g.gps_key) AS gps_events,
--     ROUND(AVG(g.speed_kmph), 2) AS avg_speed_kmph,
--     ROUND(
--         100.0 * SUM(g.is_moving) / COUNT(g.gps_key),
--         2
--     ) AS moving_rate_pct,
--     ROUND(
--         100.0 * SUM(g.is_low_signal) / COUNT(g.gps_key),
--         2
--     ) AS low_signal_rate_pct
-- FROM logistics_warehouse.fact_gps g
-- JOIN logistics_warehouse.dim_vehicle v
--     ON g.vehicle_key = v.vehicle_key
-- GROUP BY
--     v.vehicle_id,
--     v.vehicle_type
-- ORDER BY avg_speed_kmph DESC;


-- GPS activity by region:
-- SELECT
--     l.region,
--     COUNT(g.gps_key) AS gps_events,
--     COUNT(DISTINCT g.vehicle_id) AS tracked_vehicles,
--     ROUND(AVG(g.speed_kmph), 2) AS avg_speed_kmph,
--     ROUND(
--         100.0 * SUM(g.is_low_signal) / COUNT(g.gps_key),
--         2
--     ) AS low_signal_rate_pct
-- FROM logistics_warehouse.fact_gps g
-- JOIN logistics_warehouse.dim_location l
--     ON g.location_key = l.location_key
-- GROUP BY l.region
-- ORDER BY gps_events DESC;


-- Hourly fleet activity:
-- SELECT
--     EXTRACT(HOUR FROM event_timestamp) AS event_hour,
--     COUNT(*) AS gps_events,
--     COUNT(DISTINCT vehicle_id) AS active_vehicles,
--     ROUND(AVG(speed_kmph), 2) AS avg_speed_kmph
-- FROM logistics_warehouse.fact_gps
-- GROUP BY EXTRACT(HOUR FROM event_timestamp)
-- ORDER BY event_hour;


-- Daily fleet activity:
-- SELECT
--     d.full_date,
--     COUNT(g.gps_key) AS gps_events,
--     COUNT(DISTINCT g.vehicle_id) AS tracked_vehicles,
--     ROUND(AVG(g.speed_kmph), 2) AS avg_speed_kmph
-- FROM logistics_warehouse.fact_gps g
-- JOIN logistics_warehouse.dim_date d
--     ON g.date_key = d.date_key
-- GROUP BY d.full_date
-- ORDER BY d.full_date;


-- Low-signal vehicles:
-- SELECT
--     vehicle_id,
--     COUNT(*) AS gps_events,
--     SUM(is_low_signal) AS low_signal_events,
--     ROUND(
--         100.0 * SUM(is_low_signal) / COUNT(*),
--         2
--     ) AS low_signal_rate_pct
-- FROM logistics_warehouse.fact_gps
-- GROUP BY vehicle_id
-- HAVING SUM(is_low_signal) > 0
-- ORDER BY low_signal_rate_pct DESC;


-- ============================================================
-- Loading pattern
-- ============================================================
--
-- 1. Read gps_clean.csv.
-- 2. Resolve vehicle_id -> dim_vehicle.vehicle_key.
-- 3. Resolve driver assignment where reliable.
-- 4. Resolve geographic region/location -> location_key.
-- 5. Convert event timestamp -> date_key.
-- 6. Insert one telemetry row per cleaned observation.
--
-- Example conceptual query:
--
-- INSERT INTO logistics_warehouse.fact_gps (...)
-- SELECT
--     <generated_gps_key>,
--     v.vehicle_key,
--     dr.driver_key,
--     l.location_key,
--     d.date_key,
--     g.vehicle_id,
--     g.timestamp,
--     g.latitude,
--     g.longitude,
--     g.speed_kmph,
--     g.heading,
--     g.vehicle_status,
--     g.ignition_status,
--     g.signal_quality,
--     g.is_moving,
--     g.is_low_signal
-- FROM cleaned_gps g
-- LEFT JOIN dim_vehicle v
--     ON g.vehicle_id = v.vehicle_id
-- LEFT JOIN dim_driver dr
--     ON v.driver_id = dr.driver_id
-- LEFT JOIN dim_location l
--     ON g.region = l.region
-- LEFT JOIN dim_date d
--     ON CAST(g.timestamp AS DATE) = d.full_date;
--
-- The exact location matching strategy should be made more precise
-- when a production geographic master is available.
-- ============================================================


-- ============================================================
-- Relationship summary
-- ============================================================
--
--                     dim_vehicle
--                          |
--                          |
-- dim_driver ---- fact_gps ---- dim_location
--                          |
--                       dim_date
--
-- fact_gps supports:
--   * real-time/fleet monitoring
--   * vehicle movement analysis
--   * speed analysis
--   * signal-quality monitoring
--   * geographic fleet analysis
--   * route intelligence
--   * operational Power BI dashboards
--
-- It should not be used to claim exact order-level route duration
-- unless an order/GPS correlation key is introduced.
-- ============================================================
