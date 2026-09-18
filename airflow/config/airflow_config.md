# Airflow Configuration — Logistics & Supply Chain Intelligence

## 1. Purpose

This document defines the Airflow configuration and operating
conventions for the Logistics & Supply Chain Intelligence project.

Airflow is used as the workflow orchestration layer for:

- Batch ingestion and transformation
- Near-real-time Kafka/Spark processing
- Daily analytics and ML jobs
- Dependency management
- Retries and failure handling
- Pipeline observability

Airflow does **not** replace Kafka, Spark, dbt, or the data
warehouse. It coordinates those components.

---

## 2. DAGs

The project contains three DAGs.

| DAG | Purpose | Schedule |
|---|---|---|
| `logistics_batch_pipeline` | Batch ingestion + Spark cleaning + dbt | Daily |
| `realtime_pipeline` | Kafka + Spark streaming orchestration | Every 15 minutes |
| `daily_analytics_pipeline` | KPI + analytics + ML jobs | Daily at 02:00 |

### Batch pipeline

```text
Orders / Customers / Warehouses
              ↓
       Batch Ingestion
              ↓
        Spark Cleaning
              ↓
       Feature Creation
              ↓
          dbt Run
              ↓
         dbt Test
```

### Realtime pipeline

```text
GPS Producer ──→ GPS Spark Stream ──┐
                                    ├──→ Realtime Delivery
Order Producer → Order Spark Stream ┘
```

### Daily analytics pipeline

```text
Processed Data
      ↓
Operational KPI Jobs
      ↓
EDA
      ↓
Feature Engineering
   ↙       ↘
Demand      Delivery
Forecast    Prediction

Route Metrics
      ↓
Route Analysis
```

---

## 3. Airflow Installation

For a local Linux/Ubuntu environment, Airflow can be installed
inside a dedicated Python environment.

Example:

```bash
python -m venv airflow-venv
source airflow-venv/bin/activate

pip install apache-airflow
```

For a reproducible project environment, pin the Airflow version
used by the local deployment and keep the corresponding provider
packages compatible with that version.

---

## 4. Airflow Home

Set the Airflow home directory if required:

```bash
export AIRFLOW_HOME=~/airflow
```

Check:

```bash
echo $AIRFLOW_HOME
```

Initialize the metadata database:

```bash
airflow db migrate
```

---

## 5. Local Airflow Startup

Create an administrator account:

```bash
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

Start the API server:

```bash
airflow api-server --port 8080
```

Start the scheduler in another terminal:

```bash
airflow scheduler
```

The exact startup commands can vary with the installed Airflow
version. Always use the commands supported by the version installed
in the environment.

---

## 6. DAG Location

The project's DAGs are stored under:

```text
airflow/
└── dags/
    ├── logistics_batch_pipeline.py
    ├── realtime_pipeline.py
    └── daily_analytics_pipeline.py
```

For a standard Airflow installation, configure the Airflow DAG
folder to point to this directory or copy/symlink the DAG files into
the configured DAG directory.

Check the configured DAG folder:

```bash
airflow config get-value core dags_folder
```

---

## 7. Project Root

The DAGs resolve the project root relative to their own file path.

Expected project layout:

```text
logistics-supply-chain-intelligence/
├── airflow/
│   ├── dags/
│   └── config/
├── analytics/
├── data/
├── dbt/
├── ingestion/
├── kafka/
├── spark/
└── warehouse/
```

This avoids relying on a fixed absolute path such as:

```text
/home/user/project/
```

---

## 8. Environment Variables

Sensitive configuration should be provided through environment
variables or Airflow Connections/Variables.

Recommended variables include:

```text
KAFKA_BOOTSTRAP_SERVERS
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_DEFAULT_REGION
WEATHER_API_KEY
TRAFFIC_API_KEY
MAPS_API_KEY
HDFS_NAMENODE
```

Example:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export AWS_DEFAULT_REGION=ap-south-1
```

Do **not** commit real credentials to GitHub.

Use:

```text
.env
```

for local secrets and keep it excluded through `.gitignore`.

The repository contains:

```text
.env.example
```

as a configuration template.

---

## 9. Airflow Connections

External systems should preferably be represented using Airflow
Connections rather than embedding credentials inside DAG files.

Typical connections for a larger deployment could include:

```text
postgres_default
aws_default
kafka_default
```

The exact connection types depend on the providers installed in the
Airflow environment.

For this portfolio project, the DAGs primarily rely on environment
variables and command-line execution of the project's existing
scripts.

---

## 10. Retries and Failure Handling

The project DAGs use retries for transient failures.

Typical configuration:

```python
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}
```

This helps handle temporary failures such as:

- Temporary Kafka connectivity problems
- Spark startup failures
- Transient database/storage issues
- Temporary API/network failures

Retries should not be used to hide persistent data-quality or
configuration problems.

---

## 11. Scheduling

### Batch

The batch pipeline runs once per day:

```text
@daily
```

### Realtime

The realtime orchestration runs every 15 minutes:

```text
*/15 * * * *
```

This project uses bounded streaming executions for Airflow
orchestration rather than keeping an Airflow task running forever.

Continuous Kafka/Spark streaming can be operated independently in a
production architecture.

### Analytics

The daily analytics pipeline runs at:

```text
02:00 every day
```

Cron:

```text
0 2 * * *
```

---

## 12. Catchup

The project DAGs use:

```python
catchup=False
```

This prevents Airflow from automatically creating historical DAG
runs for every date between the configured `start_date` and the
current date.

For production backfills, historical runs should be triggered
deliberately after verifying data availability and idempotency.

---

## 13. Concurrency Protection

The DAGs use:

```python
max_active_runs=1
```

This prevents overlapping runs of the same DAG.

This is useful for the portfolio pipeline because it reduces the
risk of multiple runs simultaneously writing to the same local
processed-data locations.

Production deployments may use higher concurrency after storage,
partitioning, and idempotency have been designed accordingly.

---

## 14. Task Execution

The current DAGs use `BashOperator` to invoke existing project
scripts.

Examples:

```bash
python ingestion/batch/orders_ingestion.py
```

```bash
spark-submit spark/batch/clean_orders.py
```

```bash
dbt run
```

This keeps orchestration separate from transformation logic.

The same scripts can therefore be:

1. Run manually for development.
2. Run through Airflow for scheduled execution.
3. Reused in CI/CD or another orchestration platform.

---

## 15. dbt Integration

The batch pipeline executes:

```bash
cd dbt
dbt run
```

followed by:

```bash
dbt test
```

The intended flow is:

```text
Spark / Processed Data
          ↓
       dbt Models
          ↓
   Staging → Intermediate → Marts
          ↓
       dbt Tests
```

The dbt profile itself should contain environment-specific
connection information and should not be committed if it contains
credentials.

---

## 16. Monitoring Airflow

Useful Airflow checks include:

```bash
airflow dags list
```

```bash
airflow dags list-runs -d logistics_batch_pipeline
```

```bash
airflow tasks list logistics_batch_pipeline
```

DAG execution status, task duration, retries, and logs should also
be reviewed through the Airflow UI.

---

## 17. Logs

Airflow task logs are useful for diagnosing:

- Failed ingestion
- Spark errors
- Kafka connectivity issues
- dbt failures
- Data-quality failures
- Unexpected processing times

Local Airflow logs should not be committed to GitHub.

The repository `.gitignore` excludes:

```text
airflow/logs/
airflow/airflow.db
airflow/__pycache__/
```

---

## 18. Production Considerations

A production deployment would typically improve this portfolio
architecture with:

- Managed Airflow or a containerized Airflow deployment
- Remote Airflow log storage
- PostgreSQL/MySQL metadata database
- Airflow Connections and Secrets Backend
- Dedicated Spark cluster
- Managed Kafka
- S3/HDFS data lake
- Warehouse-specific dbt profile
- Alerting through email/Slack/PagerDuty
- Data-quality checks before downstream publication
- Idempotent pipeline tasks
- Dataset/partition-aware scheduling
- Backfill procedures
- CI/CD validation for DAGs

These are architectural extensions; they are not claimed as
implemented components unless represented by files elsewhere in this
repository.

---

## 19. Useful Validation Commands

Validate Python syntax:

```bash
python -m py_compile airflow/dags/logistics_batch_pipeline.py
python -m py_compile airflow/dags/realtime_pipeline.py
python -m py_compile airflow/dags/daily_analytics_pipeline.py
```

List DAGs:

```bash
airflow dags list
```

Check a DAG:

```bash
airflow dags show logistics_batch_pipeline
```

Test an individual task where supported by the installed Airflow
version:

```bash
airflow tasks test logistics_batch_pipeline ingest_orders 2026-01-01
```

---

## 20. Interview Explanation

**How is Airflow used in this project?**

> "I use Airflow as the orchestration layer. It schedules and
> coordinates batch ingestion, Spark transformations, dbt
> transformations, analytics, and ML jobs. I separated the
> workflows into batch, realtime, and daily analytics DAGs. Airflow
> handles dependencies, retries, scheduling, execution timeouts,
> and monitoring, while Kafka handles event streaming, Spark handles
> distributed processing, and dbt handles warehouse
> transformations."

**Why not put the transformation logic directly in Airflow?**

> "Airflow is an orchestrator rather than a transformation engine.
> Keeping transformation logic in reusable Python, Spark, SQL, and
> dbt scripts allows the same logic to be tested and executed
> independently of Airflow."

**How would you make it production-ready?**

> "I would move secrets to a managed secrets backend, use managed
> or containerized Airflow, store Airflow metadata in PostgreSQL,
> use remote logs, add alerting and data-quality gates, make tasks
> idempotent, and use managed Kafka/Spark and cloud storage where
> appropriate."

---

## 21. Project Reality Note

This project uses synthetic and public datasets, and some
real-time behavior is simulated.

Airflow therefore demonstrates the orchestration architecture and
workflow dependencies without claiming that the project is a
production logistics platform.

That distinction should be maintained when presenting the project
in interviews or on GitHub.
