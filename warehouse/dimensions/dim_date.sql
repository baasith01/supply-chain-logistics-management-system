-- ============================================================
-- Logistics & Supply Chain Intelligence
-- Dimension: Date
-- ============================================================
-- Grain:
--   One row per calendar date.
--
-- Purpose:
--   Provide a reusable calendar dimension for time-based analytics
--   across orders, deliveries, GPS, traffic, weather, and dashboards.
--
-- Surrogate key:
--   date_key
--
-- Recommended format:
--   YYYYMMDD
--
-- Example:
--   2026-08-15 -> 20260815
-- ============================================================

CREATE TABLE IF NOT EXISTS logistics_warehouse.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,

    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),

    week_of_year INTEGER,

    month_number INTEGER,
    month_name VARCHAR(20),

    quarter_number INTEGER,
    year_number INTEGER,

    is_weekend BOOLEAN,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_dim_date_full_date
        UNIQUE (full_date)
);


-- ============================================================
-- Date dimension population
-- ============================================================
-- The following example populates a useful calendar range for
-- the current project. The recursive CTE syntax works in many
-- modern relational warehouses but may require adaptation for
-- a specific database engine.
--
-- Project data covers 2026, while the range below provides
-- additional dates for future pipeline and forecasting work.
-- ============================================================

-- PostgreSQL-style example:
--
-- INSERT INTO logistics_warehouse.dim_date (
--     date_key,
--     full_date,
--     day_of_month,
--     day_of_week,
--     day_name,
--     week_of_year,
--     month_number,
--     month_name,
--     quarter_number,
--     year_number,
--     is_weekend
-- )
-- SELECT
--     CAST(TO_CHAR(d, 'YYYYMMDD') AS INTEGER) AS date_key,
--     d AS full_date,
--     EXTRACT(DAY FROM d)::INTEGER AS day_of_month,
--     EXTRACT(ISODOW FROM d)::INTEGER AS day_of_week,
--     TRIM(TO_CHAR(d, 'Day')) AS day_name,
--     EXTRACT(WEEK FROM d)::INTEGER AS week_of_year,
--     EXTRACT(MONTH FROM d)::INTEGER AS month_number,
--     TRIM(TO_CHAR(d, 'Month')) AS month_name,
--     EXTRACT(QUARTER FROM d)::INTEGER AS quarter_number,
--     EXTRACT(YEAR FROM d)::INTEGER AS year_number,
--     CASE
--         WHEN EXTRACT(ISODOW FROM d) IN (6, 7)
--         THEN TRUE
--         ELSE FALSE
--     END AS is_weekend
-- FROM generate_series(
--     DATE '2025-01-01',
--     DATE '2027-12-31',
--     INTERVAL '1 day'
-- ) AS series(d)
-- ON CONFLICT (date_key) DO NOTHING;


-- ============================================================
-- Data quality checks
-- ============================================================

-- Check duplicate dates:
-- SELECT full_date, COUNT(*)
-- FROM logistics_warehouse.dim_date
-- GROUP BY full_date
-- HAVING COUNT(*) > 1;

-- Check duplicate date keys:
-- SELECT date_key, COUNT(*)
-- FROM logistics_warehouse.dim_date
-- GROUP BY date_key
-- HAVING COUNT(*) > 1;

-- Check missing dates within the calendar:
-- Compare the generated calendar against the expected date range.

-- Check invalid month values:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.dim_date
-- WHERE month_number NOT BETWEEN 1 AND 12;

-- Check invalid quarter values:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.dim_date
-- WHERE quarter_number NOT BETWEEN 1 AND 4;

-- Check weekend logic:
-- SELECT
--     full_date,
--     day_name,
--     is_weekend
-- FROM logistics_warehouse.dim_date
-- ORDER BY full_date
-- LIMIT 20;


-- ============================================================
-- Example analytical queries
-- ============================================================

-- Orders by month:
-- SELECT
--     d.year_number,
--     d.month_number,
--     d.month_name,
--     COUNT(f.order_id) AS total_orders
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


-- Delivery performance by day of week:
-- SELECT
--     d.day_of_week,
--     d.day_name,
--     COUNT(f.order_id) AS total_orders,
--     ROUND(
--         AVG(f.delivery_delay_min),
--         2
--     ) AS avg_delay_min
-- FROM logistics_warehouse.fact_delivery f
-- JOIN logistics_warehouse.dim_date d
--     ON f.date_key = d.date_key
-- GROUP BY
--     d.day_of_week,
--     d.day_name
-- ORDER BY d.day_of_week;


-- Weekend vs weekday performance:
-- SELECT
--     d.is_weekend,
--     COUNT(f.order_id) AS total_orders,
--     ROUND(
--         AVG(f.delivery_delay_min),
--         2
--     ) AS avg_delay_min
-- FROM logistics_warehouse.fact_delivery f
-- JOIN logistics_warehouse.dim_date d
--     ON f.date_key = d.date_key
-- GROUP BY d.is_weekend;


-- Quarterly order value:
-- SELECT
--     d.year_number,
--     d.quarter_number,
--     ROUND(SUM(f.order_value), 2) AS total_order_value
-- FROM logistics_warehouse.fact_orders f
-- JOIN logistics_warehouse.dim_date d
--     ON f.date_key = d.date_key
-- GROUP BY
--     d.year_number,
--     d.quarter_number
-- ORDER BY
--     d.year_number,
--     d.quarter_number;


-- ============================================================
-- Relationship
-- ============================================================
--
-- fact_orders.date_key
--          |
--          v
-- dim_date.date_key
--
-- fact_delivery.date_key
--          |
--          v
-- dim_date.date_key
--
-- fact_gps.date_key
--          |
--          v
-- dim_date.date_key
--
-- fact_traffic.date_key
--          |
--          v
-- dim_date.date_key
--
-- The date dimension enables:
--   * daily trends
--   * weekly analysis
--   * monthly analysis
--   * quarterly analysis
--   * year-over-year analysis
--   * weekday/weekend comparisons
--   * Power BI time intelligence
--   * forecasting feature generation
--
-- Keeping date attributes in one conformed dimension prevents
-- repeated date logic across fact tables and dashboard queries.
-- ============================================================
