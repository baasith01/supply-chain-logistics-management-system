-- ============================================================
-- dbt staging model: stg_traffic
-- Logistics & Supply Chain Intelligence
-- ============================================================
--
-- Purpose:
--   Standardize traffic observations before they are consumed by
--   route analysis, delivery analytics, and executive marts.
--
-- Expected source columns:
--   timestamp, location, latitude, longitude, traffic_score,
--   traffic_level, average_speed_kmph
--
-- Staging remains intentionally lightweight. Business rules,
-- traffic-risk classifications, and aggregations belong in
-- downstream intermediate/mart models.
-- ============================================================

{{ config(
    materialized='view'
) }}

with source_traffic as (

    select
        timestamp,
        location,
        latitude,
        longitude,
        traffic_score,
        traffic_level,
        average_speed_kmph

    from {{ source('logistics', 'traffic') }}

),

standardized as (

    select

        -- ------------------------------------------------------
        -- Event timestamp
        -- ------------------------------------------------------
        cast(timestamp as timestamp) as event_timestamp,

        -- ------------------------------------------------------
        -- Location
        -- ------------------------------------------------------
        trim(location) as location,
        cast(latitude as decimal(10, 6)) as latitude,
        cast(longitude as decimal(10, 6)) as longitude,

        -- ------------------------------------------------------
        -- Traffic measurements
        -- ------------------------------------------------------
        cast(traffic_score as decimal(8, 2)) as traffic_score,
        lower(trim(traffic_level)) as traffic_level,
        cast(average_speed_kmph as decimal(10, 2))
            as average_speed_kmph

    from source_traffic

)

select
    event_timestamp,
    location,
    latitude,
    longitude,
    traffic_score,
    traffic_level,
    average_speed_kmph

from standardized
