# 🚚 Logistics & Supply Chain Intelligence Platform

An end-to-end **data engineering and analytics platform** designed to monitor logistics operations, analyze delivery performance, track fleet and warehouse efficiency, and support data-driven supply chain decisions.

The platform integrates **batch and real-time data pipelines** with big-data processing, data transformation, analytics, machine learning, and business intelligence.

---

## 📌 Project Overview

Modern logistics operations generate data from multiple sources, including:

- Customer orders
- Drivers
- Vehicles
- Warehouses
- GPS tracking
- Weather
- Traffic
- Delivery operations

This project builds a centralized platform to **ingest, store, process, transform, analyze, and visualize** this data.

The system supports both **batch processing** and **real-time event processing** to provide operational and executive-level insights.

---

# 🎯 Business Objectives

The platform is designed to help logistics organizations:

- Monitor order and delivery performance
- Track vehicle and driver activity
- Identify delivery delays
- Analyze regional performance
- Monitor warehouse efficiency
- Analyze logistics costs
- Understand the impact of traffic and weather
- Forecast future demand
- Improve fleet utilization
- Support data-driven operational decisions

---

# 🏗️ System Architecture

```text
                         DATA SOURCES
                              │
             ┌────────────────┼────────────────┐
             │                │                │
           Orders          GPS Data          APIs
             │                │        Weather / Traffic
             │                │                │
             └────────────────┼────────────────┘
                              ↓
                       DATA INGESTION
                       Python / APIs
                              │
                 ┌────────────┴────────────┐
                 │                         │
             Batch Data              Real-Time Data
                 │                         │
                 │                       Kafka
                 │                         │
                 └────────────┬────────────┘
                              ↓
                         DATA LAKE
                         HDFS / S3
                              ↓
                      DATA PROCESSING
                       Spark / PySpark
                              ↓
                    DATA ORGANIZATION
                           Hive
                              ↓
                      DATA WAREHOUSE
                     Dimensional Model
                              ↓
                     TRANSFORMATION
                           dbt
                              ↓
                      ORCHESTRATION
                          Airflow
                              ↓
                 ┌────────────┴────────────┐
                 │                         │
              Analytics                   ML
            Python / SQL        Forecasting / Prediction
                 │                         │
                 └────────────┬────────────┘
                              ↓
                         POWER BI
                              ↓
                    BUSINESS INTELLIGENCE
```

---

# 🧰 Technology Stack

| Category | Technology |
|---|---|
| Programming | Python |
| Querying | SQL |
| Streaming | Apache Kafka |
| Big Data Storage | Hadoop HDFS |
| Cloud Storage | AWS S3 |
| Big Data Processing | Apache Spark / PySpark |
| Big Data Querying | Apache Hive |
| Data Transformation | dbt |
| Workflow Orchestration | Apache Airflow |
| Machine Learning | Scikit-learn |
| Visualization | Microsoft Power BI |
| Monitoring | Prometheus + Grafana |
| Version Control | Git + GitHub |

---

# 📊 Data Sources

The project works with multiple logistics datasets.

## Orders

Contains customer order and delivery information.

```text
order_id
customer_id
warehouse_id
driver_id
vehicle_id
order_date
order_status
delivery_status
order_value
delivery_distance
```

## Customers

```text
customer_id
customer_name
region
customer_type
registration_date
```

## Drivers

```text
driver_id
driver_name
region
experience_years
rating
```

## Vehicles

```text
vehicle_id
vehicle_type
driver_id
fuel_type
capacity
maintenance_status
```

## Warehouses

```text
warehouse_id
warehouse_name
region
capacity
latitude
longitude
```

## GPS Tracking

```text
vehicle_id
timestamp
latitude
longitude
speed
```

## Weather

```text
timestamp
location
temperature
humidity
rainfall
weather_condition
```

## Traffic

```text
timestamp
location
traffic_level
average_speed
```

---

# 🔄 Data Pipeline

## Batch Pipeline

```text
CSV / Database
      ↓
Python Ingestion
      ↓
Raw Storage
      ↓
Spark Processing
      ↓
Cleaned Data
      ↓
Hive / Warehouse
      ↓
dbt Transformations
      ↓
Analytics
      ↓
Power BI
```

## Real-Time Pipeline

```text
GPS / Delivery Events
        ↓
Kafka Producer
        ↓
Kafka Topic
        ↓
Kafka Consumer
        ↓
Spark Structured Streaming
        ↓
Processed Streaming Data
        ↓
Analytics / Monitoring
```

---

# ⚡ Real-Time Processing

Apache Kafka is used to simulate real-time logistics events.

### Kafka Topics

```text
orders
gps_tracking
delivery_updates
traffic_updates
weather_updates
```

Example GPS event:

```json
{
  "vehicle_id": "V102",
  "timestamp": "2026-09-04T10:30:00",
  "latitude": 13.0827,
  "longitude": 80.2707,
  "speed": 42
}
```

These events can be continuously produced, consumed, and processed using Kafka and Spark Structured Streaming.

---

# 🔥 Big Data Processing

Apache Spark / PySpark is used for:

- Data cleaning
- Data transformation
- Aggregation
- Feature engineering
- Large-scale analytics
- Real-time stream processing

Example:

```text
Raw Data
   ↓
Remove Duplicates
   ↓
Handle Missing Values
   ↓
Validate Records
   ↓
Transform Data
   ↓
Create Features
   ↓
Analytical Dataset
```

---

# 🗄️ Data Warehouse Model

The analytical layer follows a **dimensional/star-schema approach**.

## Dimension Tables

```text
dim_customer
dim_driver
dim_vehicle
dim_warehouse
dim_location
dim_date
```

## Fact Tables

```text
fact_orders
fact_delivery
fact_gps
fact_traffic
```

### Simplified Model

```text
                    dim_customer
                         │
                         │
dim_date ───────── fact_orders ───────── dim_driver
                         │
                         │
                   dim_warehouse
                         │
                         │
                    dim_vehicle
```

---

# 🔧 dbt Transformation Layer

dbt is used to organize analytical transformations into structured layers.

```text
Raw Tables
    ↓
Staging Models
    ↓
Intermediate Models
    ↓
Business Marts
```

### Business Marts

```text
delivery_mart
logistics_mart
warehouse_mart
executive_mart
```

Data-quality tests are applied to important business fields and relationships.

---

# ⏱️ Airflow Orchestration

Apache Airflow is used to orchestrate scheduled data workflows.

Example pipeline:

```text
Extract
   ↓
Validate
   ↓
Load
   ↓
Spark Transformation
   ↓
dbt Transformation
   ↓
Data Quality Checks
   ↓
Analytics Dataset
```

---

# 📈 Analytics

The analytics layer focuses on several areas.

## 🚚 Delivery Analytics

- Total deliveries
- On-time delivery percentage
- Late deliveries
- Average delivery time
- Average delivery distance
- Delivery success rate

## 🚛 Fleet Analytics

- Vehicle utilization
- Vehicle performance
- Average speed
- Fuel consumption
- Maintenance status

## 👨‍✈️ Driver Analytics

- Deliveries per driver
- Driver performance
- Driver rating
- Average delivery time
- Regional driver performance

## 🏭 Warehouse Analytics

- Warehouse throughput
- Capacity utilization
- Order processing time
- Pending orders
- Regional warehouse performance

## 💰 Financial Analytics

- Revenue
- Delivery cost
- Cost per delivery
- Revenue by region
- Profitability

---

# 🤖 Machine Learning

Machine learning is applied to selected logistics use cases.

## Demand Forecasting

Historical order data is used to estimate future demand.

```text
Historical Orders
       ↓
Feature Engineering
       ↓
Machine Learning Model
       ↓
Future Demand
```

## Delivery Delay Prediction

Delivery performance can be predicted using factors such as:

```text
distance
traffic
weather
delivery time
vehicle
warehouse
```

The model can classify deliveries into categories such as:

```text
On-Time
Late
```

---

# 📊 Power BI Dashboards

The project includes multiple business intelligence dashboards.

## 1. Executive Overview

Key KPIs:

```text
Total Orders
Total Revenue
On-Time Delivery %
Average Delivery Time
Active Vehicles
Active Drivers
```

---

## 2. Delivery Operations

```text
Delivery Status
Late Deliveries
Average Delivery Time
Delay Reasons
Regional Performance
```

---

## 3. Fleet Intelligence

```text
Vehicle Utilization
Vehicle Status
Maintenance Status
Fuel Consumption
Driver Performance
```

---

## 4. Warehouse Intelligence

```text
Warehouse Throughput
Capacity Utilization
Order Processing Time
Pending Orders
Regional Warehouse Performance
```

---

## 5. Financial Intelligence

```text
Revenue
Delivery Cost
Cost per Delivery
Revenue by Region
Profitability
```

---

# 📡 Monitoring

Prometheus and Grafana are used to monitor infrastructure and pipeline health.

Example metrics:

```text
CPU Utilization
Memory Utilization
Disk Utilization
Pipeline Status
Processing Time
Kafka Activity
Failed Jobs
```

---

# 📁 Project Structure

```text
logistics-supply-chain-intelligence/
│
├── README.md
├── requirements.txt
├── .gitignore
├── .env.example
├── docker-compose.yml
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── sample/
│
├── ingestion/
│   ├── api/
│   ├── batch/
│   └── streaming/
│
├── kafka/
│   ├── producers/
│   ├── consumers/
│   └── topics.md
│
├── storage/
│   ├── hdfs/
│   └── s3/
│
├── hive/
│   ├── databases/
│   ├── tables/
│   └── queries/
│
├── spark/
│   ├── batch/
│   ├── streaming/
│   └── jobs/
│
├── warehouse/
│   ├── dimensions/
│   ├── facts/
│   └── schema.sql
│
├── dbt/
│   ├── models/
│   ├── tests/
│   └── dbt_project.yml
│
├── airflow/
│   └── dags/
│
├── analytics/
│   ├── sql/
│   └── python/
│
├── dashboards/
│   ├── powerbi/
│   └── screenshots/
│
├── monitoring/
│   ├── prometheus/
│   └── grafana/
│
├── tests/
│
├── scripts/
│
└── docs/
    ├── architecture/
    ├── database/
    ├── project/
    └── screenshots/
```

---

# 💡 Key Business Questions

The platform is designed to answer questions such as:

- Which regions experience the highest delivery delays?
- Which vehicles are underutilized?
- Which drivers have the best performance?
- Which warehouses have the highest workload?
- How does traffic affect delivery time?
- Does weather contribute to delivery delays?
- What is the average cost per delivery?
- Which regions generate the most revenue?
- Where should additional logistics capacity be allocated?
- What demand can be expected in the future?

---

# 📌 Expected Business Impact

The platform can help logistics teams:

- Improve delivery reliability
- Reduce operational delays
- Improve vehicle utilization
- Optimize warehouse capacity
- Identify high-cost operations
- Improve resource allocation
- Support demand planning
- Enable faster operational decision-making

---

# 🚀 Future Improvements

- Integrate live GPS devices
- Integrate real-time traffic APIs
- Implement advanced ETA prediction
- Add route optimization
- Add automated anomaly detection
- Deploy pipelines completely on AWS
- Implement CI/CD
- Add data lineage
- Add advanced supply-chain forecasting
- Integrate additional real-time operational data sources

---

# 👨‍💻 Skills Demonstrated

```text
Python
SQL
Data Engineering
ETL / ELT
Data Cleaning
Data Modeling
Apache Kafka
Hadoop
Hive
Apache Spark
PySpark
AWS S3
dbt
Apache Airflow
Machine Learning
Power BI
Prometheus
Grafana
Git
GitHub
```

---

# ⚠️ Project Note

This project uses **synthetic and publicly available data** for demonstration and educational purposes.

Real-time components such as GPS tracking and operational events are simulated where live production data is unavailable.

The architecture is designed to demonstrate how a logistics organization could build an end-to-end data platform combining **batch processing, real-time streaming, big-data processing, analytics, and business intelligence**.