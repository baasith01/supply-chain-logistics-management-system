-- ============================================================
-- dbt intermediate model: int_driver_performance
-- Logistics & Supply Chain Intelligence
-- ============================================================
--
-- Purpose:
--   Build a driver-level performance dataset by combining driver
--   master data with order-level delivery performance.
--
-- Grain:
--   One row per driver_id.
--
-- Upstream:
--   stg_orders
--   Existing warehouse driver data is intentionally not referenced
--   here because this dbt layer is based on standardized source
--   models. Driver attributes can be joined downstream when the
--   corresponding staging model is introduced.
--
-- Metrics:
--   - total orders
--   - completed deliveries
--   - delayed deliveries
--   - on-time delivery rate
--   - average delivery delay
--   - average promised/actual delivery time
--   - total order value
--   - average delivery distance
--   - delay rate
--
-- Risk classification is deliberately based on delivery behavior
-- rather than inventing a driver-specific risk score.
-- ============================================================

{{ config(
    materialized='view'
) }}

with orders as (

    select
        order_id,
        driver_id,
        region,
        order_status,
        delivery_status,
        order_value,
        delivery_distance_km,
        promised_delivery_time_min,
        actual_delivery_time_min

    from {{ ref('stg_orders') }}

),

driver_metrics as (

    select

        driver_id,

        -- ------------------------------------------------------
        -- Driver coverage
        -- ------------------------------------------------------
        max(region) as region,

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
                when actual_delivery_time_min
                     > promised_delivery_time_min
                then order_id
            end
        ) as delayed_deliveries,

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

        avg(
            promised_delivery_time_min
        ) as average_promised_delivery_time_min,

        avg(
            actual_delivery_time_min
        ) as average_actual_delivery_time_min,

        -- ------------------------------------------------------
        -- Commercial / operational metrics
        -- ------------------------------------------------------
        sum(
            coalesce(order_value, 0)
        ) as total_order_value,

        avg(
            delivery_distance_km
        ) as average_delivery_distance_km,

        sum(
            coalesce(delivery_distance_km, 0)
        ) as total_delivery_distance_km

    from orders

    group by driver_id

),

calculated as (

    select

        driver_id,
        region,
        total_orders,
        completed_deliveries,
        delayed_deliveries,
        average_delivery_delay_min,
        average_promised_delivery_time_min,
        average_actual_delivery_time_min,
        total_order_value,
        average_delivery_distance_km,
        total_delivery_distance_km,

        -- ------------------------------------------------------
        -- On-time delivery rate
        -- ------------------------------------------------------
        case
            when completed_deliveries > 0
            then cast(
                completed_deliveries - delayed_deliveries
                as double
            ) / completed_deliveries
            else null
        end as on_time_delivery_rate,

        -- ------------------------------------------------------
        -- Delay rate
        -- ------------------------------------------------------
        case
            when total_orders > 0
            then cast(
                delayed_deliveries
                as double
            ) / total_orders
            else null
        end as delay_rate,

        -- ------------------------------------------------------
        -- Average order value handled by driver
        -- ------------------------------------------------------
        case
            when total_orders > 0
            then cast(total_order_value as double)
                 / total_orders
            else null
        end as average_order_value

    from driver_metrics

),

classified as (

    select

        driver_id,
        region,
        total_orders,
        completed_deliveries,
        delayed_deliveries,
        average_delivery_delay_min,
        average_promised_delivery_time_min,
        average_actual_delivery_time_min,
        total_order_value,
        average_delivery_distance_km,
        total_delivery_distance_km,
        on_time_delivery_rate,
        delay_rate,
        average_order_value,

        -- ------------------------------------------------------
        -- Operational performance category
        -- ------------------------------------------------------
        case
            when total_orders = 0
                then 'No Activity'

            when on_time_delivery_rate >= 0.90
             and average_delivery_delay_min <= 5
                then 'High Performer'

            when on_time_delivery_rate >= 0.75
             and average_delivery_delay_min <= 15
                then 'Stable'

            when on_time_delivery_rate >= 0.60
                then 'Needs Attention'

            else 'At Risk'
        end as driver_performance_category

    from calculated

)

select
    driver_id,
    region,
    total_orders,
    completed_deliveries,
    delayed_deliveries,
    average_delivery_delay_min,
    average_promised_delivery_time_min,
    average_actual_delivery_time_min,
    total_order_value,
    average_delivery_distance_km,
    total_delivery_distance_km,
    on_time_delivery_rate,
    delay_rate,
    average_order_value,
    driver_performance_category

from classified
