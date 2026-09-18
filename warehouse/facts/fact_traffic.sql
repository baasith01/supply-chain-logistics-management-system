-- ============================================================
-- Logistics & Supply Chain Intelligence
-- Fact: Traffic
-- ============================================================
-- Grain:
--   One row per traffic observation at a location and timestamp.
--
-- Source:
--   data/raw/traffic.csv
--   Future traffic API ingestion:
--   ingestion/api/traffic_api.py
--
-- Purpose:
--   Store traffic observations for congestion analysis, route risk,
--   regional operations, delivery-delay investigation, and dashboards.
--
-- Fact type:
--   Event / periodic snapshot fact
--
-- Primary key:
--   traffic_key
--
-- Natural event identity:
--   location + event_timestamp
-- ============================================================

CREATE TABLE IF NOT EXISTS logistics_warehouse.fact_traffic (
    traffic_key BIGINT PRIMARY KEY,

    -- Dimension foreign keys
    location_key BIGINT,
    date_key INTEGER,

    -- Source event timestamp
    event_timestamp TIMESTAMP NOT NULL,

    -- Traffic measures
    traffic_score DECIMAL(8,2),
    traffic_level VARCHAR(50),
    average_speed_kmph DECIMAL(12,2),

    -- Audit metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_fact_traffic_location
        FOREIGN KEY (location_key)
        REFERENCES logistics_warehouse.dim_location(location_key),

    CONSTRAINT fk_fact_traffic_date
        FOREIGN KEY (date_key)
        REFERENCES logistics_warehouse.dim_date(date_key)
);


-- ============================================================
-- Recommended source-to-fact mapping
-- ============================================================
--
-- location / region       -> dim_location.location_key
-- timestamp               -> event_timestamp
-- timestamp/date          -> dim_date.date_key
-- traffic_score           -> traffic_score
-- traffic_level           -> traffic_level
-- average_speed_kmph      -> average_speed_kmph
--
-- traffic_score is expected to follow the project's normalized
-- 0-100 traffic scale:
--
--   0   -> free-flow / very low congestion
--   100 -> severe congestion
--
-- The exact interpretation should remain aligned with the
-- upstream traffic API/source.
-- ============================================================


-- ============================================================
-- Business grain
-- ============================================================
--
-- One row = one traffic observation at one location and time.
--
-- Core measures:
--   traffic_score
--   average_speed_kmph
--
-- Descriptive classification:
--   traffic_level
--
-- Common KPIs:
--   * average traffic score
--   * peak traffic score
--   * average traffic speed
--   * congestion rate
--   * severe congestion rate
--   * traffic patterns by hour/day/location
-- ============================================================


-- ============================================================
-- Data quality checks
-- ============================================================

-- Duplicate traffic observations:
-- SELECT
--     location_key,
--     event_timestamp,
--     COUNT(*) AS duplicate_count
-- FROM logistics_warehouse.fact_traffic
-- GROUP BY
--     location_key,
--     event_timestamp
-- HAVING COUNT(*) > 1;


-- Missing timestamps:
-- SELECT COUNT(*) AS missing_timestamps
-- FROM logistics_warehouse.fact_traffic
-- WHERE event_timestamp IS NULL;


-- Invalid traffic scores:
-- SELECT COUNT(*) AS invalid_scores
-- FROM logistics_warehouse.fact_traffic
-- WHERE traffic_score < 0
--    OR traffic_score > 100;


-- Invalid average speed:
-- SELECT COUNT(*) AS invalid_speeds
-- FROM logistics_warehouse.fact_traffic
-- WHERE average_speed_kmph < 0;


-- Missing locations:
-- SELECT COUNT(*) AS missing_locations
-- FROM logistics_warehouse.fact_traffic
-- WHERE location_key IS NULL;


-- Review traffic levels:
-- SELECT
--     traffic_level,
--     COUNT(*) AS observation_count
-- FROM logistics_warehouse.fact_traffic
-- GROUP BY traffic_level
-- ORDER BY observation_count DESC;


-- ============================================================
-- Example analytical queries
-- ============================================================

-- Overall traffic KPIs:
-- SELECT
--     COUNT(*) AS traffic_events,
--     ROUND(AVG(traffic_score), 2) AS avg_traffic_score,
--     ROUND(MAX(traffic_score), 2) AS max_traffic_score,
--     ROUND(AVG(average_speed_kmph), 2)
--         AS avg_traffic_speed_kmph
-- FROM logistics_warehouse.fact_traffic;


-- Traffic performance by location:
-- SELECT
--     l.location_name,
--     l.region,
--     COUNT(f.traffic_key) AS traffic_events,
--     ROUND(AVG(f.traffic_score), 2)
--         AS avg_traffic_score,
--     ROUND(AVG(f.average_speed_kmph), 2)
--         AS avg_speed_kmph
-- FROM logistics_warehouse.fact_traffic f
-- JOIN logistics_warehouse.dim_location l
--     ON f.location_key = l.location_key
-- GROUP BY
--     l.location_name,
--     l.region
-- ORDER BY avg_traffic_score DESC;


-- Congestion by location:
-- SELECT
--     l.location_name,
--     l.region,
--     COUNT(*) AS traffic_events,
--     SUM(
--         CASE
--             WHEN UPPER(f.traffic_level)
--                  IN ('HIGH', 'SEVERE')
--             THEN 1
--             ELSE 0
--         END
--     ) AS congested_events,
--     ROUND(
--         100.0 *
--         SUM(
--             CASE
--                 WHEN UPPER(f.traffic_level)
--                      IN ('HIGH', 'SEVERE')
--                 THEN 1
--                 ELSE 0
--             END
--         ) / COUNT(*),
--         2
--     ) AS congestion_rate_pct
-- FROM logistics_warehouse.fact_traffic f
-- JOIN logistics_warehouse.dim_location l
--     ON f.location_key = l.location_key
-- GROUP BY
--     l.location_name,
--     l.region
-- ORDER BY congestion_rate_pct DESC;


-- Hourly traffic pattern:
-- SELECT
--     EXTRACT(HOUR FROM event_timestamp) AS event_hour,
--     COUNT(*) AS traffic_events,
--     ROUND(AVG(traffic_score), 2)
--         AS avg_traffic_score,
--     ROUND(AVG(average_speed_kmph), 2)
--         AS avg_speed_kmph
-- FROM logistics_warehouse.fact_traffic
-- GROUP BY EXTRACT(HOUR FROM event_timestamp)
-- ORDER BY event_hour;


-- Daily traffic pattern:
-- SELECT
--     d.full_date,
--     COUNT(f.traffic_key) AS traffic_events,
--     ROUND(AVG(f.traffic_score), 2)
--         AS avg_traffic_score,
--     ROUND(AVG(f.average_speed_kmph), 2)
--         AS avg_speed_kmph
-- FROM logistics_warehouse.fact_traffic f
-- JOIN logistics_warehouse.dim_date d
--     ON f.date_key = d.date_key
-- GROUP BY d.full_date
-- ORDER BY d.full_date;


-- Severe congestion observations:
-- SELECT
--     l.location_name,
--     l.region,
--     f.event_timestamp,
--     f.traffic_score,
--     f.traffic_level,
--     f.average_speed_kmph
-- FROM logistics_warehouse.fact_traffic f
-- JOIN logistics_warehouse.dim_location l
--     ON f.location_key = l.location_key
-- WHERE UPPER(f.traffic_level) = 'SEVERE'
-- ORDER BY f.traffic_score DESC, f.event_timestamp DESC;


-- ============================================================
-- Delivery-delay investigation
-- ============================================================
-- A production implementation can correlate traffic and delivery
-- observations using a reliable geographic key and time window.
--
-- The current project should NOT join traffic directly to orders
-- solely because both datasets contain a region/location string.
-- A proper implementation should use:
--   * compatible location keys
--   * timestamp windows
--   * route/order identifiers where available
--   * or a governed geographic mapping layer
--
-- Example conceptual pattern:
--
-- SELECT
--     d.order_id,
--     d.delivery_delay_min,
--     t.traffic_score,
--     t.traffic_level
-- FROM fact_delivery d
-- JOIN fact_traffic t
--     ON d.location_key = t.location_key
--    AND t.event_timestamp BETWEEN
--        <delivery_start_time>
--        AND <delivery_end_time>;
--
-- Exact event timestamps are required before this becomes a
-- reliable causal/diagnostic analysis.
-- ============================================================


-- ============================================================
-- Loading pattern
-- ============================================================
--
-- 1. Read raw traffic observations.
-- 2. Standardize location and traffic-level values.
-- 3. Validate traffic score and speed.
-- 4. Resolve location -> location_key.
-- 5. Resolve timestamp -> date_key.
-- 6. Insert one row per cleaned traffic observation.
--
-- Example conceptual query:
--
-- INSERT INTO logistics_warehouse.fact_traffic (...)
-- SELECT
--     <generated_traffic_key>,
--     l.location_key,
--     d.date_key,
--     t.timestamp,
--     t.traffic_score,
--     t.traffic_level,
--     t.average_speed_kmph
-- FROM cleaned_traffic t
-- LEFT JOIN dim_location l
--     ON t.location = l.location_name
-- LEFT JOIN dim_date d
--     ON CAST(t.timestamp AS DATE) = d.full_date;
--
-- The exact location matching strategy should be strengthened
-- when a production geographic master is available.
-- ============================================================


-- ============================================================
-- Relationship summary
-- ============================================================
--
--                       dim_location
--                            |
--                            |
--                       fact_traffic
--                            |
--                         dim_date
--
-- fact_traffic supports:
--   * congestion monitoring
--   * traffic hotspot detection
--   * route-risk analysis
--   * hourly traffic patterns
--   * regional operations
--   * delivery-delay investigation
--   * Power BI route/logistics dashboards
-- ============================================================
