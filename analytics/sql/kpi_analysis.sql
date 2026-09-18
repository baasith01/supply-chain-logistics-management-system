-- ============================================================
-- Analytics SQL: KPI Analysis
-- Logistics & Supply Chain Intelligence
-- ============================================================
--
-- Purpose:
--   Provide reusable SQL queries for executive and operational KPI
--   analysis using the analytics-ready dbt marts.
--
-- Primary source:
--   dbt.marts.delivery_mart
--   dbt.marts.logistics_mart
--   dbt.marts.warehouse_mart
--   dbt.marts.executive_mart
--
-- Note:
--   Schema qualification may need to be adjusted according to the
--   target warehouse/dbt adapter configuration.
-- ============================================================


-- ============================================================
-- 1. Executive KPI Snapshot
-- ============================================================
-- Returns the single executive KPI record.

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
    active_vehicles_observed,
    moving_vehicles_observed,
    average_vehicle_speed_kmph,
    average_traffic_score,
    warehouses_observed,
    high_risk_warehouses,
    overall_delivery_status,
    overall_delivery_risk
from executive_mart;


-- ============================================================
-- 2. Orders, Deliveries and Order Value by Region
-- ============================================================

select
    region,
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
    sum(coalesce(order_value, 0)) as total_order_value,
    avg(order_value) as average_order_value
from delivery_mart
group by region
order by total_orders desc;


-- ============================================================
-- 3. On-Time Delivery KPI by Region
-- ============================================================

select
    region,
    count(distinct order_id) as total_orders,
    count(
        distinct case
            when is_delayed = 0
            then order_id
        end
    ) as on_time_orders,
    count(
        distinct case
            when is_delayed = 1
            then order_id
        end
    ) as delayed_orders,
    case
        when count(distinct order_id) > 0
        then cast(
            count(
                distinct case
                    when is_delayed = 0
                    then order_id
                end
            ) as double
        ) / count(distinct order_id)
        else null
    end as on_time_rate
from delivery_mart
group by region
order by on_time_rate asc;


-- ============================================================
-- 4. Average Delivery Delay by Region
-- ============================================================

select
    region,
    avg(delivery_delay_min) as average_delivery_delay_min,
    max(delivery_delay_min) as maximum_delivery_delay_min,
    count(
        distinct case
            when is_delayed = 1
            then order_id
        end
    ) as delayed_orders
from delivery_mart
group by region
order by average_delivery_delay_min desc;


-- ============================================================
-- 5. Daily Delivery Trend
-- ============================================================

select
    order_date,
    count(distinct order_id) as total_orders,
    count(
        distinct case
            when is_delayed = 1
            then order_id
        end
    ) as delayed_orders,
    avg(delivery_delay_min) as average_delay_min,
    sum(coalesce(order_value, 0)) as total_order_value
from delivery_mart
group by order_date
order by order_date;


-- ============================================================
-- 6. Delay Category Distribution
-- ============================================================

select
    delay_category,
    count(distinct order_id) as order_count,
    sum(coalesce(order_value, 0)) as order_value
from delivery_mart
group by delay_category
order by order_count desc;


-- ============================================================
-- 7. Weather vs Delivery Performance
-- ============================================================
-- Compares delivery performance across weather conditions.

select
    weather_condition,
    count(distinct order_id) as total_orders,
    avg(delivery_delay_min) as average_delay_min,
    avg(
        case
            when is_delayed = 0
            then 1.0
            else 0.0
        end
    ) as on_time_rate,
    avg(rainfall_mm) as average_rainfall_mm
from delivery_mart
where weather_condition is not null
group by weather_condition
order by average_delay_min desc;


-- ============================================================
-- 8. Heavy Rain Impact
-- ============================================================

select
    heavy_rain_flag,
    count(distinct order_id) as total_orders,
    avg(delivery_delay_min) as average_delay_min,
    avg(
        case
            when is_delayed = 0
            then 1.0
            else 0.0
        end
    ) as on_time_rate
from delivery_mart
group by heavy_rain_flag
order by heavy_rain_flag;


-- ============================================================
-- 9. Delivery Risk Distribution
-- ============================================================

select
    delivery_risk_category,
    count(distinct order_id) as order_count,
    sum(coalesce(order_value, 0)) as order_value
from delivery_mart
group by delivery_risk_category
order by order_count desc;


-- ============================================================
-- 10. Highest-Value Regions
-- ============================================================

select
    region,
    sum(coalesce(order_value, 0)) as total_order_value,
    count(distinct order_id) as total_orders,
    avg(order_value) as average_order_value
from delivery_mart
group by region
order by total_order_value desc;


-- ============================================================
-- 11. Fleet Activity KPI
-- ============================================================

select
    region,
    count(*) as gps_observations,
    count(distinct vehicle_id) as vehicles_observed,
    avg(speed_kmph) as average_speed_kmph,
    avg(traffic_score) as average_traffic_score,
    sum(severe_traffic_flag) as severe_traffic_observations,
    sum(congested_traffic_flag) as congested_traffic_observations
from logistics_mart
group by region
order by congested_traffic_observations desc;


-- ============================================================
-- 12. Traffic Risk Distribution
-- ============================================================

select
    traffic_risk_category,
    count(*) as gps_observations,
    count(distinct vehicle_id) as vehicles_observed,
    avg(speed_kmph) as average_speed_kmph,
    avg(traffic_score) as average_traffic_score
from logistics_mart
group by traffic_risk_category
order by gps_observations desc;


-- ============================================================
-- 13. Warehouse KPI Ranking
-- ============================================================

select
    warehouse_id,
    region,
    total_orders,
    completed_deliveries,
    total_order_value,
    completion_rate,
    delay_rate,
    average_delivery_delay_min,
    workload_category,
    warehouse_performance_category,
    warehouse_risk_category
from warehouse_mart
order by delay_rate desc, total_orders desc;


-- ============================================================
-- 14. High-Risk Warehouses
-- ============================================================

select
    warehouse_id,
    region,
    total_orders,
    delay_rate,
    average_delivery_delay_min,
    completion_rate,
    warehouse_risk_category
from warehouse_mart
where warehouse_risk_category in (
    'High',
    'Critical'
)
order by delay_rate desc;


-- ============================================================
-- 15. Top Delayed Orders
-- ============================================================
-- Useful for operational investigation.

select
    order_id,
    region,
    warehouse_id,
    driver_id,
    vehicle_id,
    order_value,
    delivery_distance_km,
    promised_delivery_time_min,
    actual_delivery_time_min,
    delivery_delay_min,
    delay_category,
    weather_condition,
    rainfall_mm,
    delivery_risk_category
from delivery_mart
where is_delayed = 1
order by delivery_delay_min desc
limit 100;


-- ============================================================
-- 16. KPI Summary by Delivery Distance Category
-- ============================================================

select
    distance_category,
    count(distinct order_id) as total_orders,
    avg(delivery_distance_km) as average_distance_km,
    avg(delivery_delay_min) as average_delay_min,
    avg(
        case
            when is_delayed = 0
            then 1.0
            else 0.0
        end
    ) as on_time_rate,
    sum(coalesce(order_value, 0)) as total_order_value
from delivery_mart
group by distance_category
order by average_distance_km;


-- ============================================================
-- 17. Monthly KPI Summary
-- ============================================================
-- Extracts year/month from order_date for trend analysis.

select
    extract(year from order_date) as order_year,
    extract(month from order_date) as order_month,
    count(distinct order_id) as total_orders,
    sum(coalesce(order_value, 0)) as total_order_value,
    avg(delivery_delay_min) as average_delay_min,
    avg(
        case
            when is_delayed = 0
            then 1.0
            else 0.0
        end
    ) as on_time_rate
from delivery_mart
group by
    extract(year from order_date),
    extract(month from order_date)
order by
    order_year,
    order_month;


-- ============================================================
-- 18. Executive Exception List
-- ============================================================
-- Highlights operational areas requiring management attention.

select
    warehouse_id,
    region,
    total_orders,
    delay_rate,
    completion_rate,
    average_delivery_delay_min,
    warehouse_risk_category
from warehouse_mart
where warehouse_risk_category in (
    'High',
    'Critical'
)
   or delay_rate > 0.20
order by
    delay_rate desc,
    average_delivery_delay_min desc;
