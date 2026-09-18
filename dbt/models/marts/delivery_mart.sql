-- ============================================================
-- dbt mart model: delivery_mart
-- Logistics & Supply Chain Intelligence
-- ============================================================
--
-- Purpose:
--   Create a business-facing delivery mart for Power BI,
--   operational reporting, and executive analytics.
--
-- Grain:
--   One row per order_id.
--
-- Upstream:
--   int_delivery
--
-- Design:
--   The intermediate model contains reusable transformation logic.
--   This mart exposes a clean, analytics-ready delivery dataset
--   with KPI-friendly dimensions and measures.
-- ============================================================

{{ config(
    materialized='table'
) }}

with delivery as (

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

    from {{ ref('int_delivery') }}

),

delivery_mart as (

    select

        -- ------------------------------------------------------
        -- Business dimensions
        -- ------------------------------------------------------
        order_id,
        customer_id,
        warehouse_id,
        driver_id,
        vehicle_id,
        order_date,
        region,
        order_status,
        delivery_status,

        -- ------------------------------------------------------
        -- Core delivery measures
        -- ------------------------------------------------------
        order_value,
        delivery_distance_km,
        promised_delivery_time_min,
        actual_delivery_time_min,
        delivery_delay_min,

        -- ------------------------------------------------------
        -- Delivery performance indicators
        -- ------------------------------------------------------
        is_delayed,
        delay_ratio,
        delivery_value_per_km,

        case
            when is_delayed = 0
                then 'On Time'
            when delivery_delay_min <= 15
                then 'Minor Delay'
            when delivery_delay_min <= 30
                then 'Moderate Delay'
            else 'Severe Delay'
        end as delay_category,

        case
            when delivery_distance_km < 5
                then 'Short'
            when delivery_distance_km < 15
                then 'Medium'
            when delivery_distance_km < 30
                then 'Long'
            else 'Very Long'
        end as distance_category,

        -- ------------------------------------------------------
        -- Weather context
        -- ------------------------------------------------------
        weather_date,
        weather_location,
        temperature_c,
        humidity_pct,
        rainfall_mm,
        weather_condition,
        heavy_rain_flag,
        rainy_flag,
        high_humidity_flag,

        case
            when heavy_rain_flag = 1
                then 'High Weather Risk'
            when rainy_flag = 1
              or high_humidity_flag = 1
                then 'Moderate Weather Risk'
            when weather_date is null
                then 'Unknown'
            else 'Low Weather Risk'
        end as weather_risk_category,

        -- ------------------------------------------------------
        -- Combined operational risk
        -- ------------------------------------------------------
        case
            when is_delayed = 1
             and heavy_rain_flag = 1
                then 'Critical'

            when is_delayed = 1
             and (
                    rainy_flag = 1
                    or high_humidity_flag = 1
                 )
                then 'High'

            when is_delayed = 1
                then 'Medium'

            else 'Low'
        end as delivery_risk_category

    from delivery

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
    delay_category,
    distance_category,
    weather_date,
    weather_location,
    temperature_c,
    humidity_pct,
    rainfall_mm,
    weather_condition,
    heavy_rain_flag,
    rainy_flag,
    high_humidity_flag,
    weather_risk_category,
    delivery_risk_category

from delivery_mart
