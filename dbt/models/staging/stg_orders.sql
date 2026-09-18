-- ============================================================
-- dbt staging model: stg_orders
-- Logistics & Supply Chain Intelligence
-- ============================================================
--
-- Purpose:
--   Standardize the raw orders dataset before it is consumed by
--   intermediate models and business-facing marts.
--
-- Expected source:
--   data/raw/orders.csv
--
-- Warehouse layer:
--   This model is intentionally lightweight. Business logic such
--   as delivery performance, risk classification, and KPI
--   aggregation belongs in the intermediate/mart layers.
--
-- Note:
--   The source relation is represented through the dbt source
--   macro. Define the corresponding source in a schema.yml file
--   when connecting this project to the warehouse.
-- ============================================================

{{ config(
    materialized='view'
) }}

with source_orders as (

    select
        order_id,
        customer_id,
        warehouse_id,
        driver_id,
        vehicle_id,
        order_date,
        region,
        order_status,
        delivery_status,
        order_value,
        delivery_distance_km,
        promised_delivery_time_min,
        actual_delivery_time_min

    from {{ source('logistics', 'orders') }}

),

standardized as (

    select

        -- ------------------------------------------------------
        -- Identifiers
        -- ------------------------------------------------------
        cast(order_id as string) as order_id,
        cast(customer_id as string) as customer_id,
        cast(warehouse_id as string) as warehouse_id,
        cast(driver_id as string) as driver_id,
        cast(vehicle_id as string) as vehicle_id,

        -- ------------------------------------------------------
        -- Date / geographic attributes
        -- ------------------------------------------------------
        cast(order_date as date) as order_date,
        trim(region) as region,

        -- ------------------------------------------------------
        -- Status attributes
        -- ------------------------------------------------------
        lower(trim(order_status)) as order_status,
        lower(trim(delivery_status)) as delivery_status,

        -- ------------------------------------------------------
        -- Numeric measures
        -- ------------------------------------------------------
        cast(order_value as decimal(18, 2)) as order_value,
        cast(delivery_distance_km as decimal(12, 2))
            as delivery_distance_km,
        cast(promised_delivery_time_min as integer)
            as promised_delivery_time_min,
        cast(actual_delivery_time_min as integer)
            as actual_delivery_time_min

    from source_orders

)

select
    order_id,
    customer_id,
    warehouse_id,
    driver_id,
    vehicle_id,
    order_date,
    region,
    order_status,
    delivery_status,
    order_value,
    delivery_distance_km,
    promised_delivery_time_min,
    actual_delivery_time_min

from standardized
