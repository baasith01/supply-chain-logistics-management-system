-- ============================================================
-- dbt staging model: stg_weather
-- Logistics & Supply Chain Intelligence
-- ============================================================
--
-- Purpose:
--   Standardize weather observations before they are used by
--   delivery, route, risk, and executive analytics models.
--
-- Expected source columns:
--   date, location, latitude, longitude, temperature_c,
--   humidity_pct, rainfall_mm, weather_condition
--
-- The raw weather_clean dataset also contains derived fields
-- such as is_rainy, heavy_rain_flag, high_humidity_flag and
-- weather_risk. Those business-ready features are intentionally
-- not recreated here; downstream models should own business
-- logic and feature calculations.
-- ============================================================

{{ config(
    materialized='view'
) }}

with source_weather as (

    select
        date,
        location,
        latitude,
        longitude,
        temperature_c,
        humidity_pct,
        rainfall_mm,
        weather_condition

    from {{ source('logistics', 'weather') }}

),

standardized as (

    select

        -- ------------------------------------------------------
        -- Date and location
        -- ------------------------------------------------------
        cast(date as date) as weather_date,
        trim(location) as location,

        -- ------------------------------------------------------
        -- Geographic coordinates
        -- ------------------------------------------------------
        cast(latitude as decimal(10, 6)) as latitude,
        cast(longitude as decimal(10, 6)) as longitude,

        -- ------------------------------------------------------
        -- Weather measurements
        -- ------------------------------------------------------
        cast(temperature_c as decimal(8, 2)) as temperature_c,
        cast(humidity_pct as decimal(8, 2)) as humidity_pct,
        cast(rainfall_mm as decimal(10, 2)) as rainfall_mm,

        -- ------------------------------------------------------
        -- Weather condition
        -- ------------------------------------------------------
        lower(trim(weather_condition)) as weather_condition

    from source_weather

)

select
    weather_date,
    location,
    latitude,
    longitude,
    temperature_c,
    humidity_pct,
    rainfall_mm,
    weather_condition

from standardized
