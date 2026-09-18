# 🚚 Logistics & Supply Chain Intelligence Platform

An end-to-end **Big Data and Data Engineering platform** designed to analyze logistics operations, delivery performance, supply chain efficiency, transportation costs, and external factors affecting deliveries.

The platform demonstrates a production-style data pipeline that ingests data from multiple sources, processes both batch and streaming data, stores data in a data lake, transforms it using distributed processing, loads analytical data into a cloud data warehouse, and provides business intelligence through Power BI.

---

## 📌 Project Overview

Modern logistics companies generate data from multiple sources such as:

* Orders
* Shipments
* Delivery vehicles
* Warehouses
* Drivers
* GPS/location data
* Weather conditions
* Holidays
* Fuel/energy prices
* Traffic and geographic information

Analyzing these sources independently makes it difficult to understand the overall supply chain.

This project builds a centralized analytics platform that integrates these datasets and provides insights into:

* Delivery performance
* Transportation efficiency
* Warehouse operations
* Route performance
* Delivery delays
* Logistics costs
* Demand patterns
* External factors affecting delivery

---

# 🎯 Objectives

* Build an end-to-end data engineering pipeline
* Integrate data from multiple sources
* Support both batch and real-time data ingestion
* Store large-scale data using a data lake architecture
* Process data using Apache Spark
* Perform distributed analytics using PySpark
* Build analytical tables in a cloud data warehouse
* Implement transformation workflows using dbt
* Automate pipelines using Apache Airflow
* Create business intelligence dashboards using Power BI
* Generate actionable logistics and supply-chain insights

---

# 🏗️ System Architecture

```text
                     DATA SOURCES
                         │
        ┌────────────────┼────────────────┐
        │                │                │
     APIs            CSV Files        Databases
        │                │                │
        └────────────────┼────────────────┘
                         ↓
                  Python Ingestion
                         │
             ┌───────────┴───────────┐
             │                       │
        Batch Pipeline         Streaming Pipeline
             │                       │
             │                     Kafka
             │                       │
             └───────────┬───────────┘
                         ↓
                  Data Lake Layer
                  HDFS / Amazon S3
                         ↓
                  Apache Hive
                         ↓
                Spark / PySpark
                         ↓
              Processed / Curated Data
                         ↓
              Snowflake / Redshift
                         ↓
                       dbt
                         ↓
                 Analytics Layer
                         ↓
                    Power BI
                         ↓
              Business Intelligence
```

---

# 🛠️ Technology Stack

| Technology               | Purpose                                 |
| ------------------------ | --------------------------------------- |
| **Python**               | Data ingestion and preprocessing        |
| **Pandas**               | Data manipulation                       |
| **Apache Kafka**         | Real-time data streaming                |
| **HDFS / Amazon S3**     | Data lake storage                       |
| **Apache Hive**          | Data lake querying and metadata         |
| **Apache Spark**         | Distributed data processing             |
| **PySpark**              | Large-scale transformations             |
| **Snowflake / Redshift** | Cloud data warehouse                    |
| **dbt**                  | SQL-based data transformations          |
| **Apache Airflow**       | Pipeline orchestration                  |
| **Power BI**             | Business intelligence and visualization |
| **MySQL**                | Relational data / metadata storage      |
| **Git & GitHub**         | Version control                         |

---

# 📊 Data Sources

The platform is designed to integrate several internal and external data sources.

### 🚚 Logistics Data

```text
Orders
Shipments
Deliveries
Vehicles
Drivers
Warehouses
Routes
```

### 🌦️ External Data

```text
Weather APIs
Historical Weather
Holiday Calendars
Geographic / Location Data
Fuel / Energy Data
```

Potential external sources include:

* OpenWeatherMap
* Meteostat
* U.S. Energy Information Administration
* Python Holidays
* OpenStreetMap / OSMnx

---

# 🔄 Data Engineering Pipeline

## 1. Data Ingestion

Python scripts collect data from APIs, files, and structured sources.

```text
API / CSV / Database
        ↓
Python Ingestion
        ↓
Raw Data
```

The ingestion layer is responsible for:

* Extracting data
* Schema validation
* Data type validation
* Timestamp handling
* API response processing
* Error handling
* Logging

---

# 2. Real-Time Streaming

Apache Kafka is used to simulate real-time logistics events.

Example events:

```text
Order Created
Shipment Dispatched
Vehicle Started
Delivery Updated
Delivery Completed
Delivery Delayed
```

Example Kafka architecture:

```text
Producer
   ↓
Kafka Topic
   ↓
Consumer
   ↓
Processing Layer
```

Potential Kafka topics:

```text
orders
shipments
deliveries
vehicle_events
weather_events
```

---

# 3. Data Lake

Raw data is stored in a centralized data lake.

```text
Data Sources
     ↓
Kafka / Python
     ↓
HDFS / S3
```

Example structure:

```text
data-lake/
│
├── raw/
│   ├── orders/
│   ├── shipments/
│   ├── deliveries/
│   ├── vehicles/
│   ├── weather/
│   └── holidays/
│
├── processed/
│   ├── orders/
│   ├── shipments/
│   └── deliveries/
│
└── curated/
    ├── logistics/
    ├── operations/
    └── finance/
```

---

# 4. Apache Hive

Hive provides a SQL-based interface over data stored in HDFS.

Example tables:

```text
orders
shipments
deliveries
vehicles
drivers
warehouses
weather
holidays
```

Hive is used for:

* Schema definition
* Data exploration
* Aggregation
* Batch analytics
* Data warehouse-style querying

---

# 5. Spark / PySpark Processing

Apache Spark processes large datasets using distributed computing.

Major processing tasks include:

* Data cleansing
* Deduplication
* Schema validation
* Data integration
* Aggregation
* Feature engineering
* Joining logistics and external datasets

Example:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

spark = SparkSession.builder \
    .appName("LogisticsAnalytics") \
    .getOrCreate()

deliveries = spark.read.parquet(
    "data/processed/deliveries"
)

result = deliveries.groupBy(
    "region"
).agg(
    avg("delivery_time").alias("avg_delivery_time")
)

result.show()
```

---

# 6. Data Warehouse

Processed data is loaded into a cloud data warehouse such as:

```text
Snowflake
      OR
Amazon Redshift
```

The warehouse provides optimized analytical access for BI workloads.

---

# ⭐ Data Warehouse Model

A dimensional model is used for analytical reporting.

### Fact Tables

```text
fact_orders
fact_deliveries
fact_shipments
fact_transportation
```

### Dimension Tables

```text
dim_customer
dim_vehicle
dim_driver
dim_warehouse
dim_location
dim_date
dim_weather
```

Example:

```text
                 dim_customer
                      │
                      │
dim_date ─────── fact_orders ─────── dim_location
                      │
                      │
                dim_warehouse
```

---

# 7. dbt Transformation Layer

dbt is used to transform warehouse data using modular SQL models.

Example structure:

```text
dbt_project/
│
├── models/
│   ├── staging/
│   │   ├── stg_orders.sql
│   │   ├── stg_deliveries.sql
│   │   └── stg_shipments.sql
│   │
│   ├── intermediate/
│   │   └── int_delivery_metrics.sql
│   │
│   └── marts/
│       ├── delivery_performance.sql
│       ├── logistics_cost.sql
│       └── warehouse_performance.sql
│
├── tests/
├── macros/
└── dbt_project.yml
```

dbt helps implement:

* Modular transformations
* Data testing
* Documentation
* Lineage
* Reusable SQL models

---

# 8. Airflow Orchestration

Apache Airflow coordinates the complete pipeline.

Example DAG:

```text
Extract Data
     ↓
Validate Data
     ↓
Load Raw Data
     ↓
Run Spark Processing
     ↓
Load Warehouse
     ↓
Run dbt Models
     ↓
Data Quality Tests
     ↓
Refresh Analytics
```

Example scheduling:

```text
Hourly
Daily
Weekly
```

depending on the data source and business requirement.

---

# 📈 Power BI Dashboards

The final analytics layer provides interactive dashboards for logistics stakeholders.

## 🚚 1. Delivery Performance

Key metrics:

* Total Deliveries
* On-Time Delivery Rate
* Average Delivery Time
* Delayed Deliveries
* Cancelled Deliveries
* Delivery Success Rate

Analysis:

* Delivery performance by region
* Delivery time trends
* Delay patterns
* Route performance
* Driver performance

---

## 🏭 2. Warehouse Intelligence

Key metrics:

* Orders Processed
* Average Processing Time
* Warehouse Throughput
* Inventory Movement
* Delayed Shipments

Analysis:

* Warehouse performance
* Regional demand
* Processing bottlenecks
* Shipment volume
* Capacity utilization

---

## 🚛 3. Fleet & Transportation Intelligence

Key metrics:

* Active Vehicles
* Vehicle Utilization
* Distance Travelled
* Fuel Consumption
* Transportation Cost

Analysis:

* Vehicle utilization
* Driver performance
* Route efficiency
* Fuel consumption
* Transportation costs

---

## 💰 4. Logistics Financial Intelligence

Key metrics:

* Total Logistics Cost
* Cost per Delivery
* Transportation Cost
* Fuel Cost
* Cost by Region

Analysis:

* Cost trends
* Cost by route
* Cost by vehicle
* Regional cost comparison
* High-cost delivery segments

---

# 🌦️ Weather Impact Analysis

External weather data is integrated with logistics data to understand how environmental conditions affect operations.

Example factors:

```text
Temperature
Rainfall
Wind Speed
Weather Condition
Visibility
```

Potential analysis:

```text
Weather
   +
Delivery Data
   ↓
Delay Analysis
```

This can help identify relationships between weather conditions and delivery delays.

---

# 🗺️ Route & Location Analysis

Geographic information can be used to analyze:

* Delivery locations
* Warehouse locations
* Route distances
* Regional demand
* Delivery density
* High-delay areas

OpenStreetMap and OSMnx can be used to obtain geographic and network-related information.

---

# 📊 Key KPIs

| KPI                        | Description                                                  |
| -------------------------- | ------------------------------------------------------------ |
| **On-Time Delivery Rate**  | Percentage of deliveries completed within the expected time  |
| **Average Delivery Time**  | Average time required to complete a delivery                 |
| **Delivery Delay Rate**    | Percentage of deliveries experiencing delays                 |
| **Order Fulfillment Rate** | Percentage of orders successfully fulfilled                  |
| **Vehicle Utilization**    | Percentage of available vehicle capacity/time being utilized |
| **Cost per Delivery**      | Average logistics cost associated with each delivery         |
| **Warehouse Throughput**   | Number of shipments/orders processed                         |
| **Average Route Distance** | Average distance travelled per delivery                      |
| **Fuel Consumption**       | Fuel used during transportation                              |
| **Cancellation Rate**      | Percentage of cancelled orders                               |

---

# 💡 Business Insights

The platform is designed to identify patterns such as:

* Regions experiencing higher delivery delays
* Routes with inefficient transportation performance
* Warehouses experiencing higher processing times
* Vehicles with low utilization
* Drivers with different delivery performance levels
* Locations with high order demand
* Weather conditions associated with increased delays
* High-cost transportation routes
* Peak demand periods
* Operational bottlenecks

---

# 🚀 Business Recommendations

Potential actions based on the analytics include:

### Delivery

* Optimize delivery allocation based on historical demand
* Monitor high-delay regions
* Improve delivery planning during peak periods

### Fleet

* Improve vehicle utilization
* Identify inefficient routes
* Monitor fuel consumption
* Optimize vehicle allocation

### Warehouses

* Identify processing bottlenecks
* Allocate resources based on demand
* Improve capacity planning

### Transportation

* Analyze high-cost routes
* Optimize route planning
* Monitor cost per delivery

### Weather

* Incorporate weather forecasts into delivery planning
* Prepare additional operational capacity during adverse conditions

---

# 📁 Project Structure

```text
Logistics-Supply-Chain-Intelligence/
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
│   └── topics/
│
├── hadoop/
│   ├── hdfs_upload/
│   └── hive/
│
├── spark/
│   ├── cleaning/
│   ├── transformation/
│   └── analytics/
│
├── warehouse/
│   ├── snowflake/
│   └── redshift/
│
├── dbt/
│   ├── models/
│   ├── tests/
│   ├── macros/
│   └── dbt_project.yml
│
├── airflow/
│   └── dags/
│
├── analytics/
│   ├── sql/
│   └── kpi_queries/
│
├── powerbi/
│   └── Logistics_Dashboard.pbix
│
├── screenshots/
│
├── requirements.txt
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```

---

# 🔐 Environment Configuration

Sensitive credentials should not be committed to GitHub.

Create a `.env` file locally:

```text
OPENWEATHER_API_KEY=
MYSQL_HOST=
MYSQL_USER=
MYSQL_PASSWORD=
MYSQL_DATABASE=

KAFKA_BOOTSTRAP_SERVERS=

SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=
```

Use `.env.example` as the template.

---

# ⚙️ Installation

## Clone the Repository

```bash
git clone https://github.com/your-username/logistics-supply-chain-intelligence.git

cd logistics-supply-chain-intelligence
```

## Install Dependencies

```bash
pip install -r requirements.txt
```

## Start Infrastructure

If Docker Compose is configured:

```bash
docker-compose up -d
```

## Run Ingestion

```bash
python ingestion/batch/ingest_orders.py
```

## Run Spark Processing

```bash
spark-submit spark/transformation/process_logistics.py
```

## Run dbt

```bash
dbt run
```

Run data tests:

```bash
dbt test
```

## Start Airflow

```bash
airflow scheduler
airflow webserver
```

---

# 🧪 Data Quality

The pipeline includes checks for:

* Null values
* Duplicate records
* Invalid timestamps
* Invalid IDs
* Invalid geographic values
* Schema mismatches
* Unexpected categorical values
* Referential integrity

dbt tests can be used to validate warehouse models.

---

# 📊 Example Analytical Questions

The platform can answer questions such as:

### Operations

* What is the current on-time delivery rate?
* Which regions have the highest delays?
* What are the peak delivery periods?

### Fleet

* Which vehicles have the highest utilization?
* Which routes consume the most fuel?
* What is the average distance per delivery?

### Warehouse

* Which warehouse processes the most shipments?
* Where are the largest processing bottlenecks?
* How does warehouse performance vary by region?

### Finance

* What is the cost per delivery?
* Which routes have the highest transportation costs?
* How does logistics cost change over time?

### External Factors

* How does weather affect delivery time?
* Which weather conditions are associated with delays?
* How do holidays affect demand?

---

# 🔮 Future Improvements

Potential future enhancements include:

* Real-time Power BI streaming
* ML-based delivery delay prediction
* Demand forecasting
* Dynamic route optimization
* Real-time vehicle tracking
* Anomaly detection
* Automated alerting
* AWS cloud deployment
* Databricks integration
* CI/CD for data pipelines
* Data catalog and lineage
* Advanced supply-chain optimization

---

# 🧠 Skills Demonstrated

### Data Engineering

* ETL / ELT
* Batch Processing
* Stream Processing
* Data Lakes
* Data Warehousing
* Data Modeling
* Pipeline Orchestration
* Data Quality

### Big Data

* Hadoop
* HDFS
* Hive
* Apache Spark
* PySpark
* Apache Kafka

### Cloud & Warehouse

* Snowflake
* Amazon Redshift
* Amazon S3

### Analytics

* SQL
* Python
* Power BI
* DAX
* KPI Development
* Business Intelligence

### Engineering

* Git
* GitHub
* Docker
* Airflow
* dbt

---

# 🏆 Project Highlights

```text
                    NOVANEST
                       │
                       ↓
             Logistics Data Sources
                       │
                       ↓
               Python Ingestion
                       │
             ┌─────────┴─────────┐
             ↓                   ↓
           Kafka              Batch
             │                   │
             └─────────┬─────────┘
                       ↓
                  HDFS / S3
                       ↓
                     Hive
                       ↓
                Spark / PySpark
                       ↓
              Snowflake / Redshift
                       ↓
                     dbt
                       ↓
                   Airflow
                       ↓
                  Power BI
                       ↓
             Business Intelligence
```

---

# 👨‍💻 Author

**Sulthan Baasith Z**

B.Tech Computer Science & Engineering
VIT-AP University

### Interests

* Data Engineering
* Data Analytics
* Business Intelligence
* Big Data
* Cloud Data Platforms
* Data Science

---

## ⭐ Project Goal

The goal of this project is to demonstrate how modern data engineering technologies can be combined to build a scalable **logistics and supply-chain analytics platform**, from raw data ingestion to business intelligence.
