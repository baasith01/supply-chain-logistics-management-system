# Logistics Supply Chain Intelligence Dashboard

## 1. Dashboard Overview

The Power BI dashboard provides an executive and operational view of the
Logistics & Supply Chain Intelligence Platform.

The dashboard is designed to answer:

- How much delivery activity is the business handling?
- Are deliveries being completed on time?
- Which regions and warehouses have the highest workload?
- Which drivers require operational attention?
- Where are traffic and route risks concentrated?
- How is order value distributed across the network?
- Which operational areas should management prioritize?

The dashboard uses the project's analytics and warehouse outputs rather than
directly exposing every raw dataset.

---

# 2. Recommended Dashboard Pages

Create the following five Power BI pages.

1. Executive Overview
2. Delivery Operations
3. Fleet Intelligence
4. Warehouse Intelligence
5. Financial Analysis

These pages correspond to the project's frozen dashboard structure.

---

# 3. Data Sources

Recommended Power BI source tables:

## Primary analytical tables

- `delivery_mart`
- `logistics_mart`
- `warehouse_mart`
- `executive_mart`

## Supporting dimensions

- `dim_customer`
- `dim_driver`
- `dim_vehicle`
- `dim_warehouse`
- `dim_location`
- `dim_date`

## Optional supporting datasets

- `weather`
- `traffic`
- `gps_tracking`
- `delivery_updates`

For a clean dashboard, prefer the marts as the main reporting layer and use
dimensions for filtering/drill-down.

---

# 4. Recommended Data Model

Use a star-schema approach.

### Core relationships

```text
dim_date
   │
   ├────────────── fact_orders
   │
   ├────────────── fact_delivery
   │
   └────────────── fact_gps

dim_customer ──── fact_orders

dim_driver ────── fact_orders
       │
       └───────── fact_delivery

dim_vehicle ───── fact_orders
       │
       └───────── fact_gps

dim_warehouse ─── fact_orders
       │
       └───────── fact_delivery

dim_location ──── fact_gps
       │
       └───────── fact_traffic
```

Use one-to-many relationships from dimensions to facts wherever the natural
key supports the relationship.

Avoid creating many-to-many relationships unless they are genuinely required.

---

# 5. Page 1 — Executive Overview

## Purpose

Provide senior management with a single-page summary of network performance.

## KPI Cards

Create these cards at the top:

### Total Orders

```DAX
Total Orders =
COUNTROWS(fact_orders)
```

### Total Order Value

```DAX
Total Order Value =
SUM(fact_orders[order_value])
```

### Average Order Value

```DAX
Average Order Value =
AVERAGE(fact_orders[order_value])
```

### Average Delivery Delay

```DAX
Average Delivery Delay =
AVERAGE(fact_delivery[delivery_delay_min])
```

### Delayed Orders

```DAX
Delayed Orders =
CALCULATE(
    COUNTROWS(fact_delivery),
    fact_delivery[is_delayed] = 1
)
```

### On-Time Rate

```DAX
On Time Rate =
DIVIDE(
    CALCULATE(
        COUNTROWS(fact_delivery),
        fact_delivery[is_delayed] = 0
    ),
    COUNTROWS(fact_delivery)
)
```

Format `On Time Rate` as a percentage.

## Recommended Visuals

### Orders by Region
- Clustered bar chart
- Axis: `region`
- Value: `Total Orders`

### Order Value by Region
- Column chart
- Axis: `region`
- Value: `Total Order Value`

### Delivery Performance
- Donut chart
- Legend: delayed/on-time status
- Value: order count

### Daily Order Trend
- Line chart
- X-axis: date
- Y-axis: total orders

### Executive Risk Table

Columns:

- Region
- Orders
- Delayed Orders
- Delay Rate
- Average Delay
- Order Value

Sort by highest delay rate.

---

# 6. Page 2 — Delivery Operations

## Purpose

Analyze delivery execution and identify causes or areas of poor performance.

## KPI Cards

### Total Deliveries

```DAX
Total Deliveries =
COUNTROWS(fact_delivery)
```

### Delayed Deliveries

```DAX
Delayed Deliveries =
CALCULATE(
    COUNTROWS(fact_delivery),
    fact_delivery[is_delayed] = 1
)
```

### Average Delay

```DAX
Average Delay =
AVERAGE(fact_delivery[delivery_delay_min])
```

### Maximum Delay

```DAX
Maximum Delay =
MAX(fact_delivery[delivery_delay_min])
```

### Delay Rate

```DAX
Delay Rate =
DIVIDE(
    [Delayed Deliveries],
    [Total Deliveries]
)
```

## Recommended Visuals

### Delivery Delay by Region
- Bar chart
- Region
- Average delay

### Delivery Distance vs Delay
- Scatter plot
- X-axis: delivery distance
- Y-axis: delivery delay
- Size: order value

### Delay Severity
Use categories:

- On Time
- Minor Delay
- Moderate Delay
- Severe Delay
- Critical Delay

### Weather Impact
- Column chart
- Weather condition
- Average delivery delay

### High-Risk Orders Table

Columns:

- Order ID
- Region
- Driver ID
- Warehouse ID
- Distance
- Delay
- Weather Risk
- Delivery Risk

Sort by delay descending.

---

# 7. Page 3 — Fleet Intelligence

## Purpose

Monitor vehicle and driver performance.

## KPI Cards

### Active Drivers

```DAX
Active Drivers =
DISTINCTCOUNT(fact_orders[driver_id])
```

### Active Vehicles

```DAX
Active Vehicles =
DISTINCTCOUNT(fact_gps[vehicle_id])
```

### Average Driver Delay

```DAX
Average Driver Delay =
AVERAGE(fact_delivery[delivery_delay_min])
```

### Average GPS Speed

```DAX
Average GPS Speed =
AVERAGE(fact_gps[speed_kmph])
```

### Low Signal Observations

```DAX
Low Signal Observations =
CALCULATE(
    COUNTROWS(fact_gps),
    fact_gps[is_low_signal] = 1
)
```

## Recommended Visuals

### Driver Performance Ranking
Table:

- Driver ID
- Orders
- On-Time Rate
- Average Delay
- Order Value
- Performance Category

### Driver Workload
- Bar chart
- Driver ID
- Total orders

### Vehicle Speed Distribution
- Histogram or column chart
- Speed band
- GPS observations

### Vehicle Status
- Donut or stacked column
- Vehicle status
- Vehicle count

### GPS Activity by Region
- Map if geographic fields are available
- Otherwise use a bar chart.

### Driver Risk Matrix

Use:

- X-axis: total orders
- Y-axis: average delay
- Bubble size: order value

This helps identify high-workload drivers with poor delivery performance.

---

# 8. Page 4 — Warehouse Intelligence

## Purpose

Understand warehouse workload and downstream delivery performance.

Important:

The current project data measures warehouse order workload and delivery
outcomes. It does **not** provide true physical inventory utilization.

Do not label order volume as "inventory utilization."

## KPI Cards

### Total Warehouses

```DAX
Total Warehouses =
DISTINCTCOUNT(fact_orders[warehouse_id])
```

### Orders Processed

```DAX
Warehouse Orders =
COUNTROWS(fact_orders)
```

### Average Orders per Warehouse

```DAX
Average Orders per Warehouse =
DIVIDE(
    [Warehouse Orders],
    [Total Warehouses]
)
```

### Warehouse Delay Rate

```DAX
Warehouse Delay Rate =
DIVIDE(
    CALCULATE(
        COUNTROWS(fact_delivery),
        fact_delivery[is_delayed] = 1
    ),
    COUNTROWS(fact_delivery)
)
```

## Recommended Visuals

### Warehouse Workload
- Bar chart
- Warehouse name
- Total orders

### Warehouse Order Value
- Column chart
- Warehouse
- Total order value

### Warehouse Delay Rate
- Bar chart
- Warehouse
- Delay rate

### Regional Warehouse Workload
- Stacked or clustered bar chart
- Region
- Total orders

### Warehouse Risk Table

Columns:

- Warehouse
- Region
- Orders
- Order Value
- Delayed Orders
- Delay Rate
- Average Delay
- Risk Category

---

# 9. Page 5 — Financial Analysis

## Purpose

Connect operational activity with order value and financial exposure.

## KPI Cards

### Total Revenue / Order Value

```DAX
Total Revenue =
SUM(fact_orders[order_value])
```

This project uses order value as the available financial proxy.

### Average Order Value

```DAX
Average Order Value =
AVERAGE(fact_orders[order_value])
```

### Orders

```DAX
Financial Orders =
COUNTROWS(fact_orders)
```

### Revenue per Kilometer

```DAX
Revenue per KM =
DIVIDE(
    SUM(fact_orders[order_value]),
    SUM(fact_orders[delivery_distance_km])
)
```

## Recommended Visuals

### Order Value by Region
- Column chart

### Order Value Trend
- Line chart
- Date
- Total order value

### Order Value by Warehouse
- Bar chart

### Order Value by Driver
- Bar chart

### Value Segment
Use:

- Low
- Medium
- High
- Very High

### Revenue Contribution

Use a Pareto-style view:

- Customer / driver / warehouse
- Total order value
- Contribution percentage

---

# 10. Recommended Slicers

Place slicers consistently across pages.

Recommended:

- Date
- Region
- Warehouse
- Driver
- Vehicle
- Delivery Status
- Weather Condition
- Risk Category

Use synchronized slicers where appropriate.

---

# 11. Recommended Color Semantics

Keep dashboard styling consistent.

Use semantic colors rather than decorative colors:

- Green → healthy / on-time / low risk
- Yellow → warning / moderate risk
- Orange → high risk
- Red → critical / delayed

Do not overuse colors.

The dashboard should remain professional and consulting-oriented.

---

# 12. Recommended Page Layout

Use this structure for each page:

```text
┌─────────────────────────────────────────────────────────────┐
│ PAGE TITLE                                  Last Refresh     │
├──────────┬──────────┬──────────┬──────────┬─────────────────┤
│ KPI 1    │ KPI 2    │ KPI 3    │ KPI 4    │ KPI 5           │
├──────────┴──────────┴──────────┴──────────┴─────────────────┤
│                                                             │
│                    PRIMARY TREND / KPI                      │
│                                                             │
├──────────────────────────────┬──────────────────────────────┤
│                              │                              │
│       ANALYSIS CHART         │       ANALYSIS CHART         │
│                              │                              │
├──────────────────────────────┴──────────────────────────────┤
│                    EXCEPTION / DETAIL TABLE                 │
└─────────────────────────────────────────────────────────────┘
```

Keep important management KPIs above the fold.

---

# 13. Executive Insights to Surface

The dashboard should make it easy to identify:

### Demand

- Increasing or decreasing order volume
- High-demand regions
- High-demand warehouses

### Delivery

- Overall on-time performance
- Regions with repeated delays
- Long-distance delivery problems
- Weather-related delivery deterioration

### Fleet

- Drivers with unusually high delay rates
- High-workload drivers
- Vehicle activity
- GPS/signal quality problems

### Warehouse

- High-workload warehouses
- Warehouses with high downstream delay
- Regional workload imbalance

### Financial

- Highest-value regions
- Highest-value warehouses
- Highest-value customers
- Value concentration

---

# 14. Drill-Through Recommendations

Where practical, configure drill-through pages for:

## Driver

Driver → Orders → Delay → Region → Vehicle

## Warehouse

Warehouse → Orders → Delay → Region → Value

## Region

Region → Warehouses → Drivers → Deliveries → Value

This gives the dashboard an executive-to-operational navigation path.

---

# 15. Dashboard Refresh

Recommended refresh concept:

```text
Raw / Streaming Data
        ↓
Ingestion
        ↓
Storage
        ↓
Spark Processing
        ↓
Warehouse
        ↓
dbt Marts
        ↓
Power BI
```

For the current project, refresh frequency depends on the actual environment.

The streaming components simulate real-time event ingestion, while Power BI
should be described according to the actual refresh configuration used.

Do not claim true real-time Power BI streaming unless it has actually been
configured.

---

# 16. Data Quality Checks Before Publishing

Before publishing the dashboard, verify:

- No duplicate order IDs
- No unexpected delivery statuses
- No negative order values
- No invalid delivery distances
- No impossible GPS coordinates
- No invalid warehouse IDs
- No invalid driver IDs
- No unexpected null values in key fields
- Date relationships work correctly
- Measures return expected totals
- Delayed + on-time counts reconcile with total deliveries

---

# 17. Suggested Dashboard KPIs

The final dashboard should prioritize these metrics:

| Area | KPI |
|---|---|
| Executive | Total Orders |
| Executive | Total Order Value |
| Executive | Average Order Value |
| Delivery | On-Time Rate |
| Delivery | Delay Rate |
| Delivery | Average Delay |
| Delivery | Maximum Delay |
| Fleet | Active Drivers |
| Fleet | Active Vehicles |
| Fleet | Average Driver Delay |
| Fleet | Average GPS Speed |
| Warehouse | Orders per Warehouse |
| Warehouse | Warehouse Delay Rate |
| Financial | Revenue per KM |
| Financial | Value by Region |

---

# 18. Interview Explanation

A concise explanation:

> "I designed the Power BI layer as the presentation and decision-support
> layer of the pipeline. Instead of connecting the dashboard directly to raw
> operational files, I use cleaned analytical marts and a dimensional model.
> The dashboard has executive, delivery, fleet, warehouse, and financial
> views. I use DAX measures for KPIs such as order volume, order value,
> on-time rate, delay rate, driver workload, warehouse workload, and revenue
> per kilometer. I also added exception-oriented views so management can move
> from an executive KPI to the underlying region, driver, warehouse, or
> delivery issue."

---

# 19. Important Project Honesty

The following distinction should be maintained during the interview:

### Implemented / modeled

- Power BI dashboard design
- KPI definitions
- Analytical marts
- Delivery analysis
- Driver analysis
- Warehouse analysis
- Financial analysis
- Logistics analysis
- Data pipeline architecture

### Simulated

- Real-time event generation
- Kafka streaming where synthetic events are used
- Real-time operational signals when live production sources are unavailable

### Not claimed without actual implementation

- Production Power BI real-time streaming
- Actual warehouse inventory utilization
- Exact GPS-to-road-segment mapping
- Production-grade route optimization
- Production ML deployment

This keeps the project technically credible and defensible in an interview.
