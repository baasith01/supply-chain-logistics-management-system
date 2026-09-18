-- ============================================================
-- dbt mart model: warehouse_mart
-- Logistics & Supply Chain Intelligence
-- ============================================================
--
-- Purpose:
--   Create a warehouse-level business mart for operational and
--   executive analysis.
--
-- Grain:
--   One row per warehouse_id.
--
-- Upstream:
--   stg_orders
--
-- Important:
--   The available order data measures order activity, not physical
--   inventory. Therefore this model does NOT calculate inventory
--   utilization or warehouse stock levels.
--
-- Use cases:
--   - Warehouse workload analysis
--   - Order volume comparison
--   - Delivery performance by warehouse
--   - Revenue/order-value analysis
--   - Warehouse operational risk
--   - Power BI warehouse dashboard
-- ============================================================

{{ config(
    materialized='table'
) }}

with orders as (

    select
        order_id,
        warehouse_id,
        region,
        order_date,
        order_status,
        delivery_status,
        order_value,
        delivery_distance_km,
        promised_delivery_time_min,
        actual_delivery_time_min

    from {{ ref('stg_orders') }}

),

warehouse_metrics as (

    select

        warehouse_id,

        -- ------------------------------------------------------
        -- Warehouse geographic coverage
        -- ------------------------------------------------------
        max(region) as region,

        -- ------------------------------------------------------
        -- Workload
        -- ------------------------------------------------------
        count(distinct order_id) as total_orders,

        count(
            distinct case
                when lower(delivery_status) in (
                    'delivered',
                    'completed'
                )
                then order_id
            end
        ) as completed_deliveries,

        count(
            distinct case
                when lower(delivery_status) not in (
                    'delivered',
                    'completed'
                )
                then order_id
            end
        ) as non_completed_deliveries,

        -- ------------------------------------------------------
        -- Financial activity
        -- ------------------------------------------------------
        sum(
            coalesce(order_value, 0)
        ) as total_order_value,

        avg(
            order_value
        ) as average_order_value,

        -- ------------------------------------------------------
        -- Delivery performance
        -- ------------------------------------------------------
        avg(
            case
                when actual_delivery_time_min is not null
                 and promised_delivery_time_min is not null
                then actual_delivery_time_min
                     - promised_delivery_time_min
            end
        ) as average_delivery_delay_min,

        count(
            distinct case
                when actual_delivery_time_min
                     > promised_delivery_time_min
                then order_id
            end
        ) as delayed_orders,

        -- ------------------------------------------------------
        -- Distance / logistics activity
        -- ------------------------------------------------------
        avg(
            delivery_distance_km
        ) as average_delivery_distance_km,

        sum(
            coalesce(delivery_distance_km, 0)
        ) as total_delivery_distance_km

    from orders

    group by warehouse_id

),

calculated as (

    select

        warehouse_id,
        region,
        total_orders,
        completed_deliveries,
        non_completed_deliveries,
        total_order_value,
        average_order_value,
        average_delivery_delay_min,
        delayed_orders,
        average_delivery_distance_km,
        total_delivery_distance_km,

        -- ------------------------------------------------------
        -- Completion rate
        -- ------------------------------------------------------
        case
            when total_orders > 0
            then cast(
                completed_deliveries as double
            ) / total_orders
            else null
        end as completion_rate,

        -- ------------------------------------------------------
        -- Delay rate
        -- ------------------------------------------------------
        case
            when total_orders > 0
            then cast(
                delayed_orders as double
            ) / total_orders
            else null
        end as delay_rate,

        -- ------------------------------------------------------
        -- Average value per delivery distance
        -- ------------------------------------------------------
        case
            when total_delivery_distance_km > 0
            then cast(total_order_value as double)
                 / total_delivery_distance_km
            else null
        end as order_value_per_km

    from warehouse_metrics

),

classified as (

    select

        warehouse_id,
        region,
        total_orders,
        completed_deliveries,
        non_completed_deliveries,
        total_order_value,
        average_order_value,
        average_delivery_delay_min,
        delayed_orders,
        average_delivery_distance_km,
        total_delivery_distance_km,
        completion_rate,
        delay_rate,
        order_value_per_km,

        -- ------------------------------------------------------
        -- Warehouse workload category
        -- ------------------------------------------------------
        case
            when total_orders >= 750
                then 'Very High'
            when total_orders >= 500
                then 'High'
            when total_orders >= 250
                then 'Medium'
            else 'Low'
        end as workload_category,

        -- ------------------------------------------------------
        -- Operational performance category
        -- ------------------------------------------------------
        case
            when delay_rate <= 0.10
             and average_delivery_delay_min <= 10
                then 'Healthy'

            when delay_rate <= 0.20
             and average_delivery_delay_min <= 20
                then 'Stable'

            when delay_rate <= 0.30
                then 'Needs Attention'

            else 'At Risk'
        end as warehouse_performance_category,

        -- ------------------------------------------------------
        -- Combined operational risk
        -- ------------------------------------------------------
        case
            when delay_rate > 0.30
             and average_delivery_delay_min > 20
                then 'Critical'

            when delay_rate > 0.20
                then 'High'

            when delay_rate > 0.10
                then 'Medium'

            else 'Low'
        end as warehouse_risk_category

    from calculated

)

select
    warehouse_id,
    region,
    total_orders,
    completed_deliveries,
    non_completed_deliveries,
    total_order_value,
    average_order_value,
    average_delivery_delay_min,
    delayed_orders,
    average_delivery_distance_km,
    total_delivery_distance_km,
    completion_rate,
    delay_rate,
    order_value_per_km,
    workload_category,
    warehouse_performance_category,
    warehouse_risk_category

from classified
