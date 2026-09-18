-- ============================================================
-- dbt mart model: logistics_mart
-- Logistics & Supply Chain Intelligence
-- ============================================================
--
-- Purpose:
--   Create a business-facing logistics operations mart combining
--   delivery performance with route/traffic context.
--
-- Grain:
--   One row per GPS observation (vehicle_id + event_timestamp).
--
-- Important:
--   This mart does not claim that a GPS observation belongs to a
--   particular order or road_id. The project currently has no
--   reliable direct mapping for either relationship.
--
-- Upstream:
--   int_routes
--
-- Use cases:
--   - Fleet movement monitoring
--   - Traffic/congestion analysis
--   - Route operational risk
--   - Vehicle speed analysis
--   - Power BI logistics dashboards
-- ============================================================

{{ config(
    materialized='table'
) }}

with routes as (

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
        signal_quality,
        traffic_timestamp,
        traffic_location,
        traffic_latitude,
        traffic_longitude,
        traffic_score,
        traffic_level,
        traffic_average_speed_kmph,
        time_difference_seconds,
        is_moving,
        high_speed_flag,
        severe_traffic_flag,
        congested_traffic_flag,
        vehicle_to_traffic_speed_ratio

    from {{ ref('int_routes') }}

),

logistics_mart as (

    select

        -- ------------------------------------------------------
        -- Observation identity
        -- ------------------------------------------------------
        vehicle_id,
        event_timestamp,

        -- ------------------------------------------------------
        -- Geographic context
        -- ------------------------------------------------------
        region,
        latitude,
        longitude,

        -- ------------------------------------------------------
        -- Vehicle movement
        -- ------------------------------------------------------
        speed_kmph,
        heading,
        vehicle_status,
        ignition_status,
        signal_quality,
        is_moving,

        -- ------------------------------------------------------
        -- Traffic context
        -- ------------------------------------------------------
        traffic_timestamp,
        traffic_location,
        traffic_latitude,
        traffic_longitude,
        traffic_score,
        traffic_level,
        traffic_average_speed_kmph,
        time_difference_seconds,

        -- ------------------------------------------------------
        -- Operational indicators
        -- ------------------------------------------------------
        high_speed_flag,
        severe_traffic_flag,
        congested_traffic_flag,
        vehicle_to_traffic_speed_ratio,

        case
            when severe_traffic_flag = 1
                then 'Severe Congestion'
            when congested_traffic_flag = 1
                then 'Congested'
            when traffic_score is null
                then 'Unknown'
            else 'Normal'
        end as traffic_risk_category,

        case
            when signal_quality is null
                then 'Unknown'
            when lower(signal_quality) in ('poor', 'low')
                then 'Low Signal'
            else 'Good Signal'
        end as signal_category,

        case
            when is_moving = 1
             and congested_traffic_flag = 1
                then 'Moving in Congestion'

            when is_moving = 0
             and ignition_status = 'on'
                then 'Stationary - Ignition On'

            when is_moving = 0
                then 'Stationary'

            else 'Unknown'
        end as movement_status_category,

        case
            when vehicle_to_traffic_speed_ratio is null
                then 'No Traffic Benchmark'

            when vehicle_to_traffic_speed_ratio < 0.50
                then 'Much Slower Than Traffic'

            when vehicle_to_traffic_speed_ratio < 0.80
                then 'Slower Than Traffic'

            when vehicle_to_traffic_speed_ratio <= 1.20
                then 'Aligned With Traffic'

            else 'Faster Than Traffic'
        end as relative_speed_category

    from routes

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
    signal_quality,
    is_moving,
    traffic_timestamp,
    traffic_location,
    traffic_latitude,
    traffic_longitude,
    traffic_score,
    traffic_level,
    traffic_average_speed_kmph,
    time_difference_seconds,
    high_speed_flag,
    severe_traffic_flag,
    congested_traffic_flag,
    vehicle_to_traffic_speed_ratio,
    traffic_risk_category,
    signal_category,
    movement_status_category,
    relative_speed_category

from logistics_mart
