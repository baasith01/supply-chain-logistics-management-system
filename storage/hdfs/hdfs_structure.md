# HDFS Storage Structure

## 1. Purpose

This document defines the HDFS directory structure used by the Logistics & Supply Chain Intelligence Platform.

HDFS acts as the distributed data-lake storage layer for batch and processed datasets. The structure separates raw data from processed data so that source data can be retained while transformed datasets are produced independently.

> Note: The project can also use Amazon S3 as an alternative data-lake storage layer. HDFS is included for the Hadoop-based implementation and learning path.

---

## 2. Root Structure

The project uses the following HDFS layout:

```text
/user/<hdfs_user>/logistics/
│
├── raw/
│   ├── orders.csv
│   ├── customers.csv
│   ├── drivers.csv
│   ├── vehicles.csv
│   ├── warehouses.csv
│   ├── gps_tracking.csv
│   ├── weather.csv
│   ├── traffic.csv
│   ├── roads.csv
│   └── delivery_updates.csv
│
└── processed/
    ├── orders_clean.csv
    ├── gps_clean.csv
    ├── weather_clean.csv
    └── delivery_features.csv
```

The `<hdfs_user>` value is configurable. In the supplied upload script, it defaults to the current operating-system username.

---

## 3. Raw Layer

Path:

```text
/user/<hdfs_user>/logistics/raw/
```

The raw layer stores source datasets after ingestion with minimal transformation.

### Raw datasets

| Dataset | Purpose |
|---|---|
| `orders.csv` | Customer orders and delivery information |
| `customers.csv` | Customer master data |
| `drivers.csv` | Driver master and performance attributes |
| `vehicles.csv` | Vehicle and fleet information |
| `warehouses.csv` | Warehouse locations and capacity |
| `gps_tracking.csv` | Vehicle GPS telemetry |
| `weather.csv` | Weather observations |
| `traffic.csv` | Traffic observations |
| `roads.csv` | Road and route reference data |
| `delivery_updates.csv` | Delivery event/status updates |

The raw layer should be treated as the source-of-truth landing area for the project.

---

## 4. Processed Layer

Path:

```text
/user/<hdfs_user>/logistics/processed/
```

The processed layer contains cleaned and feature-engineered datasets produced by transformation jobs.

### Processed datasets

| Dataset | Purpose |
|---|---|
| `orders_clean.csv` | Standardized order data with delivery-delay metrics |
| `gps_clean.csv` | Cleaned GPS telemetry with movement and signal features |
| `weather_clean.csv` | Cleaned weather data with weather-risk features |
| `delivery_features.csv` | Integrated order, weather, calendar, and delivery features |

Processed data is intended to be consumed by downstream analytics, Hive/Spark jobs, and warehouse transformations.

---

## 5. Data Flow

The high-level HDFS flow is:

```text
Source Systems
     |
     v
Ingestion
     |
     v
HDFS Raw Layer
     |
     |  Spark / PySpark transformation
     v
HDFS Processed Layer
     |
     +--------> Hive
     |
     +--------> Analytics / ML
     |
     +--------> Data Warehouse
     |
     +--------> dbt
     |
     v
Power BI / Business Intelligence
```

For streaming data, Kafka acts as the event-ingestion layer before downstream processing.

```text
GPS / Order Events
       |
       v
     Kafka
       |
       v
Spark Structured Streaming
       |
       v
HDFS / Processed Storage
       |
       v
Analytics / Warehouse / Dashboards
```

---

## 6. HDFS Commands

### Create the project directories

```bash
hdfs dfs -mkdir -p /user/<hdfs_user>/logistics/raw
hdfs dfs -mkdir -p /user/<hdfs_user>/logistics/processed
```

### Upload raw data

```bash
hdfs dfs -put data/raw/*.csv /user/<hdfs_user>/logistics/raw/
```

### Upload processed data

```bash
hdfs dfs -put data/processed/*.csv /user/<hdfs_user>/logistics/processed/
```

### List raw data

```bash
hdfs dfs -ls /user/<hdfs_user>/logistics/raw
```

### List processed data

```bash
hdfs dfs -ls /user/<hdfs_user>/logistics/processed
```

### Check disk usage

```bash
hdfs dfs -du -h /user/<hdfs_user>/logistics/
```

### View a file

```bash
hdfs dfs -cat /user/<hdfs_user>/logistics/raw/orders.csv
```

### Download a file from HDFS

```bash
hdfs dfs -get /user/<hdfs_user>/logistics/processed/orders_clean.csv data/processed/
```

### Remove a file

```bash
hdfs dfs -rm /user/<hdfs_user>/logistics/raw/orders.csv
```

### Remove the complete project directory

```bash
hdfs dfs -rm -r /user/<hdfs_user>/logistics/
```

Use recursive deletion carefully because it permanently removes the HDFS data.

---

## 7. Upload Automation

The repository contains:

```text
storage/hdfs/upload_to_hdfs.sh
```

This script automates the HDFS upload process.

Run it from the project root:

```bash
bash storage/hdfs/upload_to_hdfs.sh
```

The script:

1. Checks that the `hdfs` command is available.
2. Creates the raw and processed HDFS directories.
3. Uploads CSV files from `data/raw/`.
4. Uploads CSV files from `data/processed/`.
5. Lists the resulting HDFS directories for verification.

The script supports configuration through environment variables:

```bash
export HDFS_USER=your_hdfs_user
export HDFS_NAMENODE=hdfs://localhost:9000
```

Then run:

```bash
bash storage/hdfs/upload_to_hdfs.sh
```

---

## 8. Recommended HDFS Design Principles

### Raw data should be preserved

Avoid modifying raw files directly. Transform raw data into a separate processed layer.

### Separate ingestion from transformation

Ingestion places data into the raw layer. Spark/PySpark jobs create cleaned and feature-engineered datasets.

### Keep layers understandable

For this project:

```text
raw       = source/landing data
processed = cleaned/derived data
```

This makes the pipeline easier to debug and explain during an interview.

### Prefer partitioning at scale

For a production-scale implementation, large datasets such as GPS and traffic data should be partitioned by useful fields such as date and region.

Example:

```text
/user/<hdfs_user>/logistics/raw/gps_tracking/
    date=2026-08-01/
    date=2026-08-02/
    date=2026-08-03/
```

Partitioning reduces the amount of data that downstream Spark/Hive queries need to scan.

---

## 9. HDFS vs S3

The architecture supports two data-lake storage approaches:

| HDFS | Amazon S3 |
|---|---|
| Distributed file system | Cloud object storage |
| Common in Hadoop environments | Common in cloud data platforms |
| Runs within Hadoop infrastructure | Managed by AWS |
| Uses NameNode/DataNodes | Uses S3 buckets/objects |
| Good for Hadoop learning and on-prem environments | Highly scalable cloud storage |

For this project, HDFS demonstrates the Hadoop-based data-lake implementation, while the S3 upload module provides a cloud-storage alternative.

---

## 10. Interview Explanation

A concise interview answer:

> "I used HDFS as the data-lake storage layer. I separated the data into raw and processed zones. Raw data contains ingested source datasets without major transformations, while the processed zone contains cleaned and feature-engineered data generated by Spark. This separation preserves the original data, makes the pipeline easier to debug, and allows downstream Hive, analytics, warehouse, and BI workloads to consume standardized datasets."

If asked why HDFS:

> "HDFS provides distributed storage and integrates naturally with the Hadoop and Spark ecosystem. For a cloud deployment, the same data-lake concept can be implemented using Amazon S3."

---

## 11. Current Project Scope

The current repository uses a simple two-layer HDFS structure intentionally:

```text
Raw
 |
 v
Processed
```

A production implementation could extend this to:

```text
Raw
 |
 v
Staging
 |
 v
Curated
 |
 v
Analytics / Serving
```

The current design is kept lightweight so that every implemented component remains understandable, testable, and explainable.
