-- ============================================================
-- dbt mart model: executive_mart
-- Logistics & Supply Chain Intelligence
-- ============================================================
--
-- Purpose:
--   Create a single executive-level KPI mart for Power BI and
--   management reporting.
--
-- Grain:
--   One row for the complete available dataset.
--
-- Upstream:
--   delivery_mart
--   logistics_mart
--   warehouse_mart
--
-- Design:
--   This model intentionally produces a compact KPI table rather
--   than repeating detailed operational records. Detailed analysis
--   remains available through the other marts.
--
-- Main executive areas:
--   - Orders and revenue
--   - Delivery performance
--   - Logistics/fleet activity
--   - Warehouse workload
--   - Operational risk
--
-- Important:
--   Revenue here represents the sum of order_value in the supplied
--   order dataset. It should not be interpreted as recognized
--   accounting revenue unless the source business definition
--   confirms that order_value represents revenue.
-- ============================================================

{{ config(
    materialized='table'
) }}

with delivery as (

    select
        order_id,
        warehouse_id,
        driver_id,
        vehicle_id,
        order_date,
        region,
        order_value,
        delivery_distance_km,
        promised_delivery_time_min,
        actual_delivery_time_min,
        delivery_delay_min,
        is_delayed,
        delivery_status,
        weather_date,
        rainfall_mm,
        heavy_rain_flag,
        rainy_flag
    from {{ ref('delivery_mart') }}

),

logistics as (

    select
        vehicle_id,
        event_timestamp,
        is_moving,
        speed_kmph,
        severe_traffic_flag,
        congested_traffic_flag,
        traffic_score
    from {{ ref('logistics_mart') }}

),

warehouse as (

    select
        warehouse_id,
        total_orders,
        total_order_value,
        completion_rate,
        delay_rate,
        warehouse_risk_category
    from {{ ref('warehouse_mart') }}

),

-- ------------------------------------------------------------
-- Delivery KPIs
-- ------------------------------------------------------------

delivery_kpis as (

    select

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

        sum(
            coalesce(order_value, 0)
        ) as total_order_value,

        avg(
            order_value
        ) as average_order_value,

        avg(
            delivery_distance_km
        ) as average_delivery_distance_km,

        sum(
            coalesce(delivery_distance_km, 0)
        ) as total_delivery_distance_km,

        avg(
            actual_delivery_time_min
        ) as average_actual_delivery_time_min,

        avg(
            promised_delivery_time_min
        ) as average_promised_delivery_time_min,

        avg(
            delivery_delay_min
        ) as average_delivery_delay_min,

        count(
            distinct case
                when is_delayed = 1
                then order_id
            end
        ) as delayed_orders,

        count(
            distinct case
                when is_delayed = 0
                then order_id
            end
        ) as on_time_orders,

        count(
            distinct case
                when heavy_rain_flag = 1
                then order_id
            end
        ) as orders_with_heavy_rain,

        count(
            distinct case
                when rainy_flag = 1
                then order_id
            end
        ) as orders_with_rain

    from delivery

),

delivery_calculated as (

    select

        *,

        case
            when completed_deliveries > 0
            then cast(
                on_time_orders as double
            ) / completed_deliveries
            else null
        end as on_time_delivery_rate,

        case
            when total_orders > 0
            then cast(
                delayed_orders as double
            ) / total_orders
            else null
        end as delay_rate,

        case
            when total_delivery_distance_km > 0
            then cast(total_order_value as double)
                 / total_delivery_distance_km
            else null
        end as order_value_per_km

    from delivery_kpis

),

-- ------------------------------------------------------------
-- Logistics KPIs
-- ------------------------------------------------------------

logistics_kpis as (

    select

        count(*) as total_gps_observations,

        count(
            distinct vehicle_id
        ) as active_vehicles_observed,

        count(
            distinct case
                when is_moving = 1
                then vehicle_id
            end
        ) as moving_vehicles_observed,

        avg(
            speed_kmph
        ) as average_vehicle_speed_kmph,

        avg(
            traffic_score
        ) as average_traffic_score,

        sum(
            severe_traffic_flag
        ) as severe_traffic_observations,

        sum(
            congested_traffic_flag
        ) as congested_traffic_observations

    from logistics

),

-- ------------------------------------------------------------
-- Warehouse KPIs
-- ------------------------------------------------------------

warehouse_kpis as (

    select

        count(
            distinct warehouse_id
        ) as warehouses_observed,

        sum(
            coalesce(total_orders, 0)
        ) as warehouse_order_volume,

        sum(
            coalesce(total_order_value, 0)
        ) as warehouse_order_value,

        avg(
            completion_rate
        ) as average_warehouse_completion_rate,

        avg(
            delay_rate
        ) as average_warehouse_delay_rate,

        count(
            distinct case
                when warehouse_risk_category in (
                    'High',
                    'Critical'
                )
                then warehouse_id
            end
        ) as high_risk_warehouses

    from warehouse

),

-- ------------------------------------------------------------
-- Final executive KPI record
-- ------------------------------------------------------------

final as (

    select

        -- ------------------------------------------------------
        -- Executive scope
        -- ------------------------------------------------------
        current_date() as report_date,

        -- ------------------------------------------------------
        -- Order / financial KPIs
        -- ------------------------------------------------------
        d.total_orders,
        d.completed_deliveries,
        d.total_order_value,
        d.average_order_value,

        -- ------------------------------------------------------
        -- Delivery KPIs
        -- ------------------------------------------------------
        d.delayed_orders,
        d.on_time_orders,
        d.on_time_delivery_rate,
        d.delay_rate,
        d.average_delivery_delay_min,
        d.average_actual_delivery_time_min,
        d.average_promised_delivery_time_min,

        -- ------------------------------------------------------
        -- Logistics KPIs
        -- ------------------------------------------------------
        d.average_delivery_distance_km,
        d.total_delivery_distance_km,
        d.order_value_per_km,

        -- ------------------------------------------------------
        -- Weather-related delivery context
        -- ------------------------------------------------------
        d.orders_with_heavy_rain,
        d.orders_with_rain,

        -- ------------------------------------------------------
        -- Fleet / traffic KPIs
        -- ------------------------------------------------------
        l.total_gps_observations,
        l.active_vehicles_observed,
        l.moving_vehicles_observed,
        l.average_vehicle_speed_kmph,
        l.average_traffic_score,
        l.severe_traffic_observations,
        l.congested_traffic_observations,

        -- ------------------------------------------------------
        -- Warehouse KPIs
        -- ------------------------------------------------------
        w.warehouses_observed,
        w.warehouse_order_volume,
        w.warehouse_order_value,
        w.average_warehouse_completion_rate,
        w.average_warehouse_delay_rate,
        w.high_risk_warehouses,

        -- ------------------------------------------------------
        -- Executive status indicators
        -- ------------------------------------------------------
        case
            when d.on_time_delivery_rate >= 0.90
                then 'Healthy'
            when d.on_time_delivery_rate >= 0.75
                then 'Stable'
            when d.on_time_delivery_rate >= 0.60
                then 'Needs Attention'
            else 'At Risk'
        end as overall_delivery_status,

        case
            when d.delay_rate <= 0.10
                then 'Low'
            when d.delay_rate <= 0.20
                then 'Medium'
            when d.delay_rate <= 0.30
                then 'High'
            else 'Critical'
        end as overall_delivery_risk

    from delivery_calculated d
    cross join logistics_kpis l
    cross join warehouse_kpis w

)

select
    report_date,
    total_orders,
    completed_deliveries,
    total_order_value,
    average_order_value,
    delayed_orders,
    on_time_orders,
    on_time_delivery_rate,
    delay_rate,
    average_delivery_delay_min,
    average_actual_delivery_time_min,
    average_promised_delivery_time_min,
    average_delivery_distance_km,
    total_delivery_distance_km,
    order_value_per_km,
    orders_with_heavy_rain,
    orders_with_rain,
    total_gps_observations,
    active_vehicles_observed,
    moving_vehicles_observed,
    average_vehicle_speed_kmph,
    average_traffic_score,
    severe_traffic_observations,
    congested_traffic_observations,
    warehouses_observed,
    warehouse_order_volume,
    warehouse_order_value,
    average_warehouse_completion_rate,
    average_warehouse_delay_rate,
    high_risk_warehouses,
    overall_delivery_status,
    overall_delivery_risk

from final
