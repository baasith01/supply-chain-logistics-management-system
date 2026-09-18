-- ============================================================
-- Logistics & Supply Chain Intelligence
-- Data Warehouse Schema
-- ============================================================
-- Purpose:
--   Define the analytical warehouse layer used by downstream dbt models,
--   SQL analytics, and Power BI.
--
-- Modeling approach:
--   Star schema:
--       Dimension tables -> descriptive business entities
--       Fact tables      -> measurable business events
--
-- Dimensions:
--   dim_customer
--   dim_driver
--   dim_vehicle
--   dim_warehouse
--   dim_location
--   dim_date
--
-- Facts:
--   fact_orders
--   fact_delivery
--   fact_gps
--   fact_traffic
--
-- Notes:
--   1. This schema is warehouse-agnostic SQL with common relational syntax.
--   2. Exact physical types/index syntax may be adjusted for PostgreSQL,
--      MySQL, Snowflake, Redshift, or another target warehouse.
--   3. Surrogate keys are used for dimensions so analytical facts are not
--      tightly coupled to source-system identifiers.
--   4. The current project does not contain inventory snapshots, so no
--      inventory-utilization fact is fabricated.
-- ============================================================


-- ============================================================
-- 1. CREATE ANALYTICAL SCHEMA
-- ============================================================

CREATE SCHEMA IF NOT EXISTS logistics_warehouse;


-- ============================================================
-- 2. DIMENSION TABLES
-- ============================================================

-- ------------------------------------------------------------
-- Customer Dimension
-- ------------------------------------------------------------

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


-- ------------------------------------------------------------
-- Driver Dimension
-- ------------------------------------------------------------

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


-- ------------------------------------------------------------
-- Vehicle Dimension
-- ------------------------------------------------------------

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


-- ------------------------------------------------------------
-- Warehouse Dimension
-- ------------------------------------------------------------

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


-- ------------------------------------------------------------
-- Location Dimension
-- ------------------------------------------------------------

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


-- ------------------------------------------------------------
-- Date Dimension
-- ------------------------------------------------------------

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

    CONSTRAINT uq_dim_date_full_date
        UNIQUE (full_date)
);


-- ============================================================
-- 3. FACT TABLES
-- ============================================================

-- ------------------------------------------------------------
-- Orders Fact
-- Grain:
--   One row per order.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS logistics_warehouse.fact_orders (
    order_key BIGINT PRIMARY KEY,

    order_id VARCHAR(50) NOT NULL,
    customer_key BIGINT,
    driver_key BIGINT,
    vehicle_key BIGINT,
    warehouse_key BIGINT,
    location_key BIGINT,
    date_key INTEGER,

    order_status VARCHAR(50),
    delivery_status VARCHAR(50),

    order_value DECIMAL(14,2),
    delivery_distance_km DECIMAL(12,2),
    promised_delivery_time_min DECIMAL(12,2),
    actual_delivery_time_min DECIMAL(12,2),
    delivery_delay_min DECIMAL(12,2),
    is_delayed INTEGER,

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


-- ------------------------------------------------------------
-- Delivery Fact
-- Grain:
--   One row per delivery/order-level delivery event.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS logistics_warehouse.fact_delivery (
    delivery_key BIGINT PRIMARY KEY,

    order_id VARCHAR(50) NOT NULL,
    customer_key BIGINT,
    driver_key BIGINT,
    vehicle_key BIGINT,
    warehouse_key BIGINT,
    location_key BIGINT,
    date_key INTEGER,

    delivery_status VARCHAR(50),
    promised_delivery_time_min DECIMAL(12,2),
    actual_delivery_time_min DECIMAL(12,2),
    delivery_delay_min DECIMAL(12,2),
    delivery_distance_km DECIMAL(12,2),

    is_delayed INTEGER,
    on_time_flag INTEGER,

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


-- ------------------------------------------------------------
-- GPS Fact
-- Grain:
--   One row per vehicle GPS observation.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS logistics_warehouse.fact_gps (
    gps_key BIGINT PRIMARY KEY,

    vehicle_key BIGINT,
    driver_key BIGINT,
    location_key BIGINT,
    date_key INTEGER,

    vehicle_id VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,

    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    speed_kmph DECIMAL(12,2),
    heading DECIMAL(8,2),

    vehicle_status VARCHAR(50),
    ignition_status VARCHAR(50),
    signal_quality VARCHAR(50),

    is_moving INTEGER,
    is_low_signal INTEGER,

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


-- ------------------------------------------------------------
-- Traffic Fact
-- Grain:
--   One row per traffic observation at a location and timestamp.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS logistics_warehouse.fact_traffic (
    traffic_key BIGINT PRIMARY KEY,

    location_key BIGINT,
    date_key INTEGER,

    event_timestamp TIMESTAMP NOT NULL,

    traffic_score DECIMAL(8,2),
    traffic_level VARCHAR(50),
    average_speed_kmph DECIMAL(12,2),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_fact_traffic_location
        FOREIGN KEY (location_key)
        REFERENCES logistics_warehouse.dim_location(location_key),

    CONSTRAINT fk_fact_traffic_date
        FOREIGN KEY (date_key)
        REFERENCES logistics_warehouse.dim_date(date_key)
);


-- ============================================================
-- 4. INDEXES FOR COMMON ANALYTICAL FILTERS
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_fact_orders_date
    ON logistics_warehouse.fact_orders(date_key);

CREATE INDEX IF NOT EXISTS idx_fact_orders_driver
    ON logistics_warehouse.fact_orders(driver_key);

CREATE INDEX IF NOT EXISTS idx_fact_orders_warehouse
    ON logistics_warehouse.fact_orders(warehouse_key);

CREATE INDEX IF NOT EXISTS idx_fact_orders_customer
    ON logistics_warehouse.fact_orders(customer_key);

CREATE INDEX IF NOT EXISTS idx_fact_delivery_date
    ON logistics_warehouse.fact_delivery(date_key);

CREATE INDEX IF NOT EXISTS idx_fact_delivery_driver
    ON logistics_warehouse.fact_delivery(driver_key);

CREATE INDEX IF NOT EXISTS idx_fact_delivery_warehouse
    ON logistics_warehouse.fact_delivery(warehouse_key);

CREATE INDEX IF NOT EXISTS idx_fact_gps_vehicle
    ON logistics_warehouse.fact_gps(vehicle_key);

CREATE INDEX IF NOT EXISTS idx_fact_gps_timestamp
    ON logistics_warehouse.fact_gps(event_timestamp);

CREATE INDEX IF NOT EXISTS idx_fact_traffic_location
    ON logistics_warehouse.fact_traffic(location_key);

CREATE INDEX IF NOT EXISTS idx_fact_traffic_timestamp
    ON logistics_warehouse.fact_traffic(event_timestamp);


-- ============================================================
-- 5. DATA QUALITY CHECKS
-- ============================================================
-- These queries are intended for validation after loading data.
-- They do not modify the warehouse.

-- Duplicate source orders:
-- SELECT order_id, COUNT(*)
-- FROM logistics_warehouse.fact_orders
-- GROUP BY order_id
-- HAVING COUNT(*) > 1;

-- Invalid delivery times:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.fact_delivery
-- WHERE actual_delivery_time_min < 0
--    OR promised_delivery_time_min < 0;

-- Invalid GPS coordinates:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.fact_gps
-- WHERE latitude NOT BETWEEN -90 AND 90
--    OR longitude NOT BETWEEN -180 AND 180;

-- Invalid traffic scores:
-- SELECT COUNT(*)
-- FROM logistics_warehouse.fact_traffic
-- WHERE traffic_score < 0
--    OR traffic_score > 100;


-- ============================================================
-- 6. BUSINESS GRAIN SUMMARY
-- ============================================================
--
-- dim_customer   -> one row per customer
-- dim_driver     -> one row per driver
-- dim_vehicle    -> one row per vehicle
-- dim_warehouse  -> one row per warehouse
-- dim_location   -> one row per analytical location
-- dim_date       -> one row per calendar date
--
-- fact_orders    -> one row per order
-- fact_delivery  -> one row per delivery/order
-- fact_gps       -> one row per GPS observation
-- fact_traffic   -> one row per traffic observation
--
-- This design supports:
--   * delivery KPIs
--   * driver performance
--   * fleet monitoring
--   * warehouse workload
--   * route and traffic analysis
--   * regional analysis
--   * executive dashboards
-- ============================================================
