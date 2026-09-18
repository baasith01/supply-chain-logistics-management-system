-- ============================================================
-- Logistics & Supply Chain Intelligence
-- Dimension: Customer
-- ============================================================
-- Grain:
--   One row per unique customer.
--
-- Source:
--   data/raw/customers.csv
--
-- Purpose:
--   Store descriptive customer attributes used by analytical facts,
--   dbt models, SQL analytics, and Power BI.
--
-- Surrogate key:
--   customer_key
--
-- Natural/source key:
--   customer_id
-- ============================================================

CREATE TABLE IF NOT EXISTS logistics_warehouse.dim_customer (
    customer_key BIGINT PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(150),
    region VARCHAR(100),
    customer_type VARCHAR(50),
    registration_date DATE,

    source_system VARCHAR(50) DEFAULT 'synthetic_source',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_dim_customer_customer_id
        UNIQUE (customer_id)
);


-- ============================================================
-- Recommended source-to-dimension mapping
-- ============================================================
--
-- customer_id       -> customer_id
-- customer_name     -> customer_name
-- region            -> region
-- customer_type     -> customer_type
-- registration_date -> registration_date
--
-- customer_key is generated in the warehouse layer and should
-- remain stable even if the source-system customer identifier
-- changes in a future implementation.
-- ============================================================


-- ============================================================
-- Data quality checks
-- ============================================================

-- Check duplicate customer IDs:
-- SELECT customer_id, COUNT(*)
-- FROM logistics_warehouse.dim_customer
-- GROUP BY customer_id
-- HAVING COUNT(*) > 1;

-- Check missing customer IDs:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.dim_customer
-- WHERE customer_id IS NULL;

-- Check invalid registration dates:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.dim_customer
-- WHERE registration_date > CURRENT_DATE;

-- Check supported customer types:
-- SELECT customer_type, COUNT(*)
-- FROM logistics_warehouse.dim_customer
-- GROUP BY customer_type
-- ORDER BY COUNT(*) DESC;


-- ============================================================
-- Example analytical queries
-- ============================================================

-- Customer count by region:
-- SELECT
--     region,
--     COUNT(*) AS customer_count
-- FROM logistics_warehouse.dim_customer
-- GROUP BY region
-- ORDER BY customer_count DESC;


-- Customer count by type:
-- SELECT
--     customer_type,
--     COUNT(*) AS customer_count
-- FROM logistics_warehouse.dim_customer
-- GROUP BY customer_type
-- ORDER BY customer_count DESC;


-- ============================================================
-- Relationship
-- ============================================================
--
-- fact_orders.customer_key
--          |
--          v
-- dim_customer.customer_key
--
-- This allows order/revenue/delivery facts to be analyzed by:
--   * customer
--   * customer type
--   * region
--   * registration cohort/date
--
-- ============================================================
