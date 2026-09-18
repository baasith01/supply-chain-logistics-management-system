-- ============================================================
-- dbt intermediate model: int_routes
-- Logistics & Supply Chain Intelligence
-- ============================================================
--
-- Purpose:
--   Build a route/traffic operational dataset by combining
--   standardized GPS observations with traffic observations.
--
-- Grain:
--   One row per GPS observation.
--
-- Important modeling decision:
--   The project does not contain a reliable direct GPS-observation
--   to road_id mapping. Therefore this model does NOT invent one.
--   Traffic is associated using the nearest traffic observation
--   in time for the same location, while route/road-network
--   relationships can be added later when a geospatial road-match
--   process is implemented.
--
-- Upstream:
--   stg_gps
--   stg_traffic
-- ============================================================

{{ config(
    materialized='view'
) }}

with gps as (

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

    from {{ ref('stg_gps') }}

),

traffic as (

    select
        event_timestamp as traffic_timestamp,
        location,
        latitude as traffic_latitude,
        longitude as traffic_longitude,
        traffic_score,
        traffic_level,
        average_speed_kmph as traffic_average_speed_kmph

    from {{ ref('stg_traffic') }}

),

-- ------------------------------------------------------------
-- Match each GPS observation to traffic observations from the
-- same region/location where possible, within a bounded time
-- window. A nearest timestamp is selected.
--
-- Because the raw traffic data is location-based and does not
-- contain vehicle_id, this creates operational context rather
-- than claiming a vehicle-specific traffic measurement.
-- ------------------------------------------------------------

candidate_matches as (

    select

        g.vehicle_id,
        g.event_timestamp,
        g.region,
        g.latitude,
        g.longitude,
        g.speed_kmph,
        g.heading,
        g.vehicle_status,
        g.ignition_status,
        g.signal_quality,

        t.traffic_timestamp,
        t.location as traffic_location,
        t.traffic_latitude,
        t.traffic_longitude,
        t.traffic_score,
        t.traffic_level,
        t.traffic_average_speed_kmph,

        abs(
            unix_timestamp(g.event_timestamp)
            - unix_timestamp(t.traffic_timestamp)
        ) as time_difference_seconds,

        row_number() over (
            partition by g.vehicle_id, g.event_timestamp
            order by abs(
                unix_timestamp(g.event_timestamp)
                - unix_timestamp(t.traffic_timestamp)
            )
        ) as traffic_rank

    from gps g

    left join traffic t
        on lower(trim(g.region)) = lower(trim(t.location))
       and t.traffic_timestamp between
           g.event_timestamp - interval 30 minutes
           and g.event_timestamp + interval 30 minutes

),

matched as (

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
        time_difference_seconds

    from candidate_matches

    where traffic_rank = 1

),

route_features as (

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

        -- ------------------------------------------------------
        -- Vehicle movement classification
        -- ------------------------------------------------------
        case
            when coalesce(speed_kmph, 0) > 0
            then 1
            else 0
        end as is_moving,

        case
            when coalesce(speed_kmph, 0) >= 60
            then 1
            else 0
        end as high_speed_flag,

        -- ------------------------------------------------------
        -- Traffic classification
        -- ------------------------------------------------------
        case
            when coalesce(traffic_score, 0) >= 75
            then 1
            else 0
        end as severe_traffic_flag,

        case
            when coalesce(traffic_score, 0) >= 50
            then 1
            else 0
        end as congested_traffic_flag,

        -- ------------------------------------------------------
        -- Speed comparison
        -- ------------------------------------------------------
        case
            when traffic_average_speed_kmph is not null
             and traffic_average_speed_kmph > 0
            then cast(speed_kmph as double)
                 / traffic_average_speed_kmph
            else null
        end as vehicle_to_traffic_speed_ratio

    from matched

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

from route_features
