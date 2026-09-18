# LogiTrack Data Model

## 1. Overview

The LogiTrack data model supports an end-to-end logistics and supply-chain analytics platform. It combines operational, delivery, fleet, warehouse, GPS, weather, traffic, and road-network data.

The model is designed to support:

- Order and delivery analytics
- Customer analysis
- Driver and fleet performance
- Warehouse performance
- GPS and route analysis
- Traffic and weather impact analysis
- Real-time delivery monitoring
- Executive KPIs
- Machine-learning feature generation

The analytical warehouse follows a **star-schema-oriented design**, while raw and streaming datasets retain their operational structures.

---

## 2. Core Entities

| Entity | Grain | Primary Key | Purpose |
|---|---|---|---|
| customers | One row per customer | customer_id | Customer master data |
| orders | One row per order | order_id | Order and commercial transaction data |
| drivers | One row per driver | driver_id | Driver master and performance attributes |
| vehicles | One row per vehicle | vehicle_id | Fleet and vehicle attributes |
| warehouses | One row per warehouse | warehouse_id | Warehouse master data |
| gps_tracking | One row per GPS observation | vehicle_id + timestamp | Vehicle movement telemetry |
| delivery_updates | One row per delivery event | event_id | Delivery lifecycle events |
| weather | One row per location/date | date + location | Weather observations |
| traffic | One row per location/timestamp | location + timestamp | Traffic observations |
| roads | One row per road segment | road_id | Road-network reference data |

---

## 3. Source Data Model

### 3.1 Customers

**Grain:** One record per customer.

Key attributes:

- `customer_id` — Primary key
- `customer_name`
- `region`
- `customer_type`
- `registration_date`

Used for customer segmentation, order frequency, revenue contribution, and regional analysis.

---

### 3.2 Orders

**Grain:** One record per order.

Key attributes:

- `order_id` — Primary key
- `customer_id` — Foreign key to customers
- `warehouse_id` — Foreign key to warehouses
- `driver_id` — Foreign key to drivers
- `vehicle_id` — Foreign key to vehicles
- `order_date`
- `region`
- `order_status`
- `delivery_status`
- `order_value`
- `delivery_distance_km`
- `promised_delivery_time_min`
- `actual_delivery_time_min`

Derived analytical fields in the processed layer include:

- `delivery_delay_min`
- `is_delayed`
- `delivery_per_km_value`

The order table is the main commercial transaction entity.

---

### 3.3 Drivers

**Grain:** One record per driver.

Key attributes:

- `driver_id` — Primary key
- `driver_name`
- `region`
- `experience_years`
- `rating`
- `employment_status`
- `license_type`

Driver performance is calculated from orders and delivery outcomes rather than storing aggregated KPIs in the master table.

---

### 3.4 Vehicles

**Grain:** One record per vehicle.

Key attributes:

- `vehicle_id` — Primary key
- `vehicle_type`
- `driver_id` — Foreign key to drivers
- `fuel_type`
- `capacity_kg`
- `manufacture_year`
- `maintenance_status`
- `vehicle_status`
- `odometer_km`

GPS observations connect to vehicles through `vehicle_id`.

---

### 3.5 Warehouses

**Grain:** One record per warehouse.

Key attributes:

- `warehouse_id` — Primary key
- `warehouse_name`
- `region`
- `capacity_units`
- `latitude`
- `longitude`

Warehouse analysis focuses on order workload, delivery performance, value contribution, and network balance. `capacity_units` is a reference attribute; it is not treated as actual inventory utilization unless inventory-level data is available.

---

### 3.6 GPS Tracking

**Grain:** One record per vehicle GPS observation.

Key attributes:

- `vehicle_id`
- `timestamp`
- `region`
- `latitude`
- `longitude`
- `speed_kmph`
- `heading`
- `vehicle_status`
- `ignition_status`
- `signal_quality`

Processed fields include:

- `date`
- `hour`
- `day_of_week`
- `is_moving`
- `is_low_signal`

The natural identifier is the combination of `vehicle_id` and `timestamp`.

---

### 3.7 Delivery Updates

**Grain:** One record per delivery lifecycle event.

Key attributes:

- `event_id` — Primary key
- `order_id` — Foreign key to orders
- `driver_id` — Foreign key to drivers
- `vehicle_id` — Foreign key to vehicles
- `event_timestamp`
- `region`
- `event_type`
- `delivery_status`
- `delay_minutes`
- `source`

Typical events include order creation, pickup, dispatch, out-for-delivery, delay, and delivery completion.

---

### 3.8 Weather

**Grain:** One observation per location and date.

Key attributes:

- `date`
- `location`
- `latitude`
- `longitude`
- `temperature_c`
- `humidity_pct`
- `rainfall_mm`
- `weather_condition`

Processed fields include:

- `is_rainy`
- `heavy_rain_flag`
- `high_humidity_flag`
- `weather_risk`

Weather is joined to delivery analytics primarily through location/region and date.

---

### 3.9 Traffic

**Grain:** One observation per location and timestamp.

Key attributes:

- `timestamp`
- `location`
- `latitude`
- `longitude`
- `traffic_score`
- `traffic_level`
- `average_speed_kmph`

`traffic_score` is normalized to a 0–100 scale for analytics.

---

### 3.10 Roads

**Grain:** One road segment.

Key attributes:

- `road_id` — Primary key
- `start_location`
- `end_location`
- `road_type`
- `distance_km`
- `lanes`
- `speed_limit_kmph`
- `road_condition`

Road data provides network context for route analysis.

**Important modeling constraint:** The current project does not invent a direct `order_id -> road_id` or `gps_observation -> road_id` relationship when such mapping is not present in the source data. Advanced map matching can be added later using actual geospatial road-network processing.

---

# 4. Relationship Model

The main logical relationships are:

```text
customers
    1
    |
    N
orders

warehouses
    1
    |
    N
orders

drivers
    1
    |
    N
orders

drivers
    1
    |
    N
vehicles

vehicles
    1
    |
    N
gps_tracking

orders
    1
    |
    N
delivery_updates

drivers
    1
    |
    N
delivery_updates

vehicles
    1
    |
    N
delivery_updates
```

Additional analytical relationships:

```text
weather
   |
   | location + date
   v
delivery / order analytics

traffic
   |
   | location + time window
   v
GPS / route analytics

roads
   |
   | road-network context
   v
route analytics
```

Weather and traffic relationships are **analytical/time-spatial relationships**, not necessarily strict foreign-key relationships.

---

# 5. Analytical Warehouse Model

The warehouse uses dimensions and facts to separate descriptive attributes from measurable business events.

## Dimensions

- `dim_customer`
- `dim_driver`
- `dim_vehicle`
- `dim_warehouse`
- `dim_location`
- `dim_date`

## Facts

- `fact_orders`
- `fact_delivery`
- `fact_gps`
- `fact_traffic`

---

## 5.1 Dimension: dim_customer

**Grain:** One row per customer.

Typical columns:

```text
customer_key       -- surrogate key
customer_id        -- business/natural key
customer_name
region
customer_type
registration_date
```

The surrogate `customer_key` is used by fact tables.

---

## 5.2 Dimension: dim_driver

**Grain:** One row per driver.

Typical columns:

```text
driver_key
driver_id
driver_name
region
experience_years
rating
employment_status
license_type
```

---

## 5.3 Dimension: dim_vehicle

**Grain:** One row per vehicle.

Typical columns:

```text
vehicle_key
vehicle_id
vehicle_type
driver_id
fuel_type
capacity_kg
manufacture_year
maintenance_status
vehicle_status
odometer_km
```

---

## 5.4 Dimension: dim_warehouse

**Grain:** One row per warehouse.

Typical columns:

```text
warehouse_key
warehouse_id
warehouse_name
region
capacity_units
latitude
longitude
```

---

## 5.5 Dimension: dim_location

**Grain:** One row per analytical location.

Typical attributes:

```text
location_key
location_name
region
latitude
longitude
```

This dimension provides a common geographic reference for weather, traffic, warehouses, and future geographic models.

---

## 5.6 Dimension: dim_date

**Grain:** One row per calendar date.

Typical attributes:

```text
date_key
full_date
year
quarter
month
month_name
week
day
day_name
day_of_week
is_weekend
```

The date dimension supports consistent time-series analysis across fact tables.

---

# 6. Fact Tables

## 6.1 fact_orders

**Grain:** One row per order.

Measures and keys include:

```text
order_key
order_id
customer_key
driver_key
vehicle_key
warehouse_key
location_key
date_key
order_status
delivery_status
order_value
delivery_distance_km
promised_delivery_time_min
actual_delivery_time_min
```

Primary business metrics:

- Total orders
- Total order value
- Average order value
- Orders by region
- Orders by warehouse
- Customer value
- Driver workload

---

## 6.2 fact_delivery

**Grain:** One row per order-level delivery outcome.

Typical attributes:

```text
delivery_key
order_id
customer_key
driver_key
vehicle_key
warehouse_key
location_key
date_key
promised_delivery_time_min
actual_delivery_time_min
delivery_delay_min
is_delayed
delivery_status
```

Primary metrics:

- On-time delivery rate
- Delay rate
- Average delay
- Maximum delay
- Delay severity
- Delivery efficiency

---

## 6.3 fact_gps

**Grain:** One row per GPS observation.

Typical attributes:

```text
gps_key
vehicle_key
location_key
date_key
timestamp
latitude
longitude
speed_kmph
heading
vehicle_status
ignition_status
signal_quality
```

The natural event identity is:

```text
vehicle_id + timestamp
```

Primary metrics:

- Average speed
- Distance/movement indicators
- Moving observations
- Low-signal observations
- Vehicle activity
- Geographic movement patterns

---

## 6.4 fact_traffic

**Grain:** One traffic observation for a location and timestamp.

Typical attributes:

```text
traffic_key
location_key
date_key
timestamp
traffic_score
traffic_level
average_speed_kmph
```

Primary metrics:

- Average traffic score
- Congested observations
- Average traffic speed
- Regional traffic risk
- Traffic impact on route conditions

---

# 7. Star Schema

The high-level warehouse model is:

```text
                    dim_customer
                         |
                         |
dim_date ---- fact_orders ---- dim_driver
                         |
                         |
                  dim_warehouse
                         |
                    dim_vehicle


                    dim_customer
                         |
                         |
dim_date ---- fact_delivery ---- dim_driver
                         |
                    dim_vehicle
                         |
                    dim_warehouse


dim_date ---- fact_gps ---- dim_vehicle
                  |
             dim_location


dim_date ---- fact_traffic ---- dim_location
```

This structure allows BI tools to filter facts through reusable dimensions.

---

# 8. Data Grain Rules

Maintaining the correct grain is critical.

| Dataset | Correct Grain |
|---|---|
| Orders | One row per order |
| Customers | One row per customer |
| Drivers | One row per driver |
| Vehicles | One row per vehicle |
| Warehouses | One row per warehouse |
| Delivery Updates | One row per delivery event |
| GPS | One row per vehicle observation |
| Weather | One row per location/date observation |
| Traffic | One row per location/timestamp observation |
| Roads | One row per road segment |
| fact_orders | One row per order |
| fact_delivery | One row per order-level delivery |
| fact_gps | One row per GPS observation |
| fact_traffic | One row per traffic observation |

Avoid joining two fact tables directly without controlling grain, because this can multiply rows and inflate KPIs.

---

# 9. Data Flow Through the Model

```text
Raw CSV / API / Streaming Data
            |
            v
       Ingestion Layer
            |
            v
       Raw Data Lake
        HDFS / S3
            |
            v
      Spark Processing
            |
            v
       Cleaned Data
            |
            v
          Hive
            |
            v
    Data Warehouse Layer
            |
            v
          dbt
            |
            v
     Analytical Marts
            |
            +------------------+
            |                  |
            v                  v
        SQL Analytics       Power BI
            |
            v
        ML Features
            |
            v
 Forecasting / Delay Prediction
```

---

# 10. dbt Model Layers

The dbt project follows three logical transformation layers.

## Staging

```text
stg_orders
stg_gps
stg_weather
stg_traffic
```

Purpose:

- Standardize source columns
- Cast data types
- Normalize fields
- Create clean source-facing models

## Intermediate

```text
int_delivery
int_routes
int_driver_performance
```

Purpose:

- Combine related datasets
- Calculate analytical features
- Prepare reusable business logic

## Marts

```text
delivery_mart
logistics_mart
warehouse_mart
executive_mart
```

Purpose:

- Provide business-ready datasets
- Simplify BI queries
- Centralize KPI logic
- Support dashboards and reporting

---

# 11. Data Quality Rules

The model should enforce the following checks:

### Primary keys

- No null primary keys
- No duplicate business keys

### Foreign keys

- `orders.customer_id` should exist in customers
- `orders.driver_id` should exist in drivers
- `orders.vehicle_id` should exist in vehicles
- `orders.warehouse_id` should exist in warehouses
- Delivery references should point to valid orders/drivers/vehicles

### Numeric validation

- Order value >= 0
- Delivery distance >= 0
- Actual delivery time >= 0
- Delay minutes >= 0
- Traffic score between 0 and 100
- GPS speed >= 0
- Rainfall >= 0

### Status validation

Delivery status should use controlled values such as:

```text
pending
in_transit
out_for_delivery
delivered
cancelled
delayed
```

The exact allowed values should remain consistent across ingestion, transformation, tests, and BI.

---

# 12. Slowly Changing Dimensions

The current project primarily uses a simple dimension approach suitable for the synthetic analytical dataset.

For a production implementation, dimensions such as:

- Driver
- Vehicle
- Customer
- Warehouse

could use **Slowly Changing Dimension Type 2 (SCD2)** when historical attribute changes need to be preserved.

Example:

```text
driver_key
driver_id
rating
region
employment_status
valid_from
valid_to
is_current
```

This would allow historical reports to use the driver attributes that were valid at the time of the delivery.

---

# 13. Real-Time Data Model

Real-time events enter through Kafka topics:

```text
orders
gps_tracking
delivery_updates
traffic_updates
weather_updates
```

A typical real-time flow is:

```text
Event Source
     |
     v
   Kafka
     |
     v
Spark Structured Streaming
     |
     +------> Real-time processing
     |
     +------> Feature generation
     |
     +------> Monitoring / alerts
     |
     v
Serving / Analytical Layer
     |
     v
Dashboards
```

The real-time model is event-oriented, while the analytical warehouse remains optimized for historical reporting.

---

# 14. Machine Learning Data Model

The processed delivery feature dataset combines relevant predictive information.

Potential feature groups:

### Order features

- Order value
- Delivery distance
- Promised delivery time
- Region
- Order date/time

### Weather features

- Temperature
- Humidity
- Rainfall
- Weather condition
- Weather risk

### Driver features

- Experience
- Rating
- Historical workload
- Historical delay rate

### Warehouse features

- Warehouse region
- Historical workload
- Historical delivery performance

### Target variables

For delay prediction:

```text
is_delayed
```

For demand forecasting:

```text
daily_order_count
```

Actual delivery outcomes must not be used as input features when predicting the outcome itself, because this would introduce target leakage.

---

# 15. Business Questions Supported

The data model supports questions such as:

### Executive

- How many orders were processed?
- What is total order value?
- What is the overall delivery delay rate?
- Which regions contribute the most value?

### Delivery

- Which regions have the highest delay rate?
- What factors are associated with delivery delays?
- How does weather affect delivery performance?

### Customer

- Who are the highest-value customers?
- Which customers have repeated delivery issues?
- What is customer value by region/type?

### Driver

- Which drivers have the best on-time performance?
- Which drivers have high workload and poor performance?
- How does experience relate to delivery performance?

### Warehouse

- Which warehouses process the most orders?
- Which warehouses have higher delay rates?
- Is workload balanced across the warehouse network?

### Fleet

- Which vehicles are most active?
- What are average vehicle speeds?
- Where are low-signal GPS observations concentrated?

### Route

- Which areas have higher traffic risk?
- How do traffic conditions vary by region?
- Which road segments require further investigation?

---

# 16. Modeling Principles

The project follows these principles:

1. **Define grain before joining datasets.**
2. **Use primary and foreign keys for trusted relationships.**
3. **Separate raw, processed, and analytical layers.**
4. **Use dimensions for reusable descriptive attributes.**
5. **Use fact tables for measurable business events.**
6. **Avoid double-counting caused by many-to-many fact joins.**
7. **Keep real-time events separate from historical warehouse aggregates where appropriate.**
8. **Do not create relationships that are not supported by source data.**
9. **Prevent target leakage in ML features.**
10. **Apply data-quality checks before business reporting.**

---

## 17. Summary

The LogiTrack data model provides a structured foundation for logistics analytics by connecting orders, customers, drivers, vehicles, warehouses, delivery events, GPS telemetry, weather, traffic, and road-network data.

The architecture separates operational ingestion from analytical consumption and provides a consistent path from:

**Raw Data → Clean Data → Hive → Warehouse → dbt Marts → Analytics/ML → Power BI**

This design supports both historical business intelligence and near-real-time logistics monitoring while keeping data grain, relationships, and analytical assumptions explicit.
