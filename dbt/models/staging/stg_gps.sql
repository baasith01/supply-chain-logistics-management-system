-- ============================================================
-- dbt staging model: stg_gps
-- Logistics & Supply Chain Intelligence
-- ============================================================
--
-- Purpose:
--   Standardize GPS tracking observations before they are used
--   by route, fleet, delivery, and operational analytics models.
--
-- Expected source columns:
--   vehicle_id, timestamp, region, latitude, longitude,
--   speed_kmph, heading, vehicle_status, ignition_status,
--   signal_quality
--
-- Business calculations and aggregations belong in the
-- intermediate/mart layers.
-- ============================================================

{{ config(
    materialized='view'
) }}

with source_gps as (

    select
        vehicle_id,
        timestamp,
        region,
        latitude,
        longitude,
        speed_kmph,
        heading,
        vehicle_status,
        ignition_status,
        signal_quality

    from {{ source('logistics', 'gps_tracking') }}

),

standardized as (

    select

        -- ------------------------------------------------------
        -- Identifiers
        -- ------------------------------------------------------
        cast(vehicle_id as string) as vehicle_id,

        -- ------------------------------------------------------
        -- Event timestamp
        -- ------------------------------------------------------
        cast(timestamp as timestamp) as event_timestamp,

        -- ------------------------------------------------------
        -- Geographic attributes
        -- ------------------------------------------------------
        trim(region) as region,
        cast(latitude as decimal(10, 6)) as latitude,
        cast(longitude as decimal(10, 6)) as longitude,

        -- ------------------------------------------------------
        -- Vehicle movement
        -- ------------------------------------------------------
        cast(speed_kmph as decimal(10, 2)) as speed_kmph,
        cast(heading as decimal(6, 2)) as heading,

        -- ------------------------------------------------------
        -- Vehicle / signal status
        -- ------------------------------------------------------
        lower(trim(vehicle_status)) as vehicle_status,
        lower(trim(ignition_status)) as ignition_status,
        lower(trim(signal_quality)) as signal_quality

    from source_gps

)

select
    vehicle_id,
    event_timestamp,
    region,
    latitude,
    longitude,
    speed_kmph,
    heading,
    vehicle_status,
    ignition_status,
    signal_quality

from standardized
