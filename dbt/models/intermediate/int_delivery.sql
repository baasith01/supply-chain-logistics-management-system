-- ============================================================
-- dbt intermediate model: int_delivery
-- Logistics & Supply Chain Intelligence
-- ============================================================
--
-- Purpose:
--   Create an order-level delivery dataset by combining the
--   standardized order data with the latest relevant weather
--   observation for the order's region/location and deriving
--   delivery-performance metrics.
--
-- Grain:
--   One row per order_id.
--
-- Important:
--   This model does not invent a direct order-to-weather key.
--   Weather is matched using the order region against weather
--   location, with the latest weather date on or before the
--   order date selected for that location.
--
-- Upstream:
--   stg_orders
--   stg_weather
-- ============================================================

{{ config(
    materialized='view'
) }}

with orders as (

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

    from {{ ref('stg_orders') }}

),

weather as (

    select
        weather_date,
        location,
        temperature_c,
        humidity_pct,
        rainfall_mm,
        weather_condition

    from {{ ref('stg_weather') }}

),

weather_ranked as (

    select
        o.order_id,
        w.weather_date,
        w.location,
        w.temperature_c,
        w.humidity_pct,
        w.rainfall_mm,
        w.weather_condition,

        row_number() over (
            partition by o.order_id
            order by w.weather_date desc
        ) as weather_rank

    from orders o

    left join weather w
        on lower(trim(o.region)) = lower(trim(w.location))
       and w.weather_date <= o.order_date

),

latest_weather as (

    select
        order_id,
        weather_date,
        location as weather_location,
        temperature_c,
        humidity_pct,
        rainfall_mm,
        weather_condition

    from weather_ranked

    where weather_rank = 1

),

delivery_metrics as (

    select

        o.order_id,
        o.customer_id,
        o.warehouse_id,
        o.driver_id,
        o.vehicle_id,
        o.order_date,
        o.region,
        o.order_status,
        o.delivery_status,
        o.order_value,
        o.delivery_distance_km,
        o.promised_delivery_time_min,
        o.actual_delivery_time_min,

        -- ------------------------------------------------------
        -- Delivery performance
        -- ------------------------------------------------------
        (
            o.actual_delivery_time_min
            - o.promised_delivery_time_min
        ) as delivery_delay_min,

        case
            when o.actual_delivery_time_min
                 > o.promised_delivery_time_min
            then 1
            else 0
        end as is_delayed,

        case
            when o.promised_delivery_time_min > 0
            then cast(
                o.actual_delivery_time_min
                - o.promised_delivery_time_min
                as double
            ) / o.promised_delivery_time_min
            else null
        end as delay_ratio,

        -- ------------------------------------------------------
        -- Delivery efficiency
        -- ------------------------------------------------------
        case
            when o.delivery_distance_km > 0
            then cast(o.order_value as double)
                 / o.delivery_distance_km
            else null
        end as delivery_value_per_km,

        -- ------------------------------------------------------
        -- Weather context
        -- ------------------------------------------------------
        w.weather_date,
        w.weather_location,
        w.temperature_c,
        w.humidity_pct,
        w.rainfall_mm,
        w.weather_condition,

        case
            when coalesce(w.rainfall_mm, 0) > 10
            then 1
            else 0
        end as heavy_rain_flag,

        case
            when coalesce(w.rainfall_mm, 0) > 0
            then 1
            else 0
        end as rainy_flag,

        case
            when coalesce(w.humidity_pct, 0) >= 80
            then 1
            else 0
        end as high_humidity_flag

    from orders o

    left join latest_weather w
        on o.order_id = w.order_id

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
    actual_delivery_time_min,
    delivery_delay_min,
    is_delayed,
    delay_ratio,
    delivery_value_per_km,
    weather_date,
    weather_location,
    temperature_c,
    humidity_pct,
    rainfall_mm,
    weather_condition,
    heavy_rain_flag,
    rainy_flag,
    high_humidity_flag

from delivery_metrics
