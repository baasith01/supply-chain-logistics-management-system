# Kafka Topics

## Overview

Apache Kafka is used in this project to simulate and process real-time
logistics events.

The streaming flow is:

```text
Data Producers
     │
     ▼
   Kafka
     │
     ├── orders
     ├── gps_tracking
     ├── delivery_updates
     ├── traffic_updates
     └── weather_updates
     │
     ▼
Kafka Consumers / Spark Structured Streaming
     │
     ▼
Processing → Storage → Analytics → Dashboards
```

## Topic Catalog

| Topic | Purpose | Producer | Typical Consumer |
|---|---|---|---|
| `orders` | Real-time order creation/status events | `order_producer.py` | Order consumer / Spark |
| `gps_tracking` | Vehicle GPS and movement events | `gps_producer.py` | GPS consumer / Spark |
| `delivery_updates` | Delivery status and delay events | Delivery event producer | Delivery processing |
| `traffic_updates` | Traffic conditions and speed updates | Traffic API producer | Route analysis |
| `weather_updates` | Weather conditions affecting logistics | Weather API producer | Delivery prediction |

---

## 1. `orders`

### Purpose

Carries order-related events such as new orders and changes in order or
delivery status.

### Example event

```json
{
  "order_id": "ORD000001",
  "customer_id": "CUST0305",
  "warehouse_id": "WH003",
  "driver_id": "DRV0019",
  "vehicle_id": "VEH0070",
  "order_date": "2026-02-05",
  "region": "Vellore",
  "order_status": "Dispatched",
  "delivery_status": "Delayed",
  "order_value": 250.35,
  "delivery_distance_km": 8.62
}
```

### Business use

- Track incoming orders
- Monitor order status
- Measure delivery performance
- Feed real-time operational dashboards

---

## 2. `gps_tracking`

### Purpose

Carries real-time vehicle location and movement events.

### Example event

```json
{
  "vehicle_id": "VEH0001",
  "timestamp": "2026-08-01 08:28:54",
  "region": "Tirunelveli",
  "latitude": 8.722217,
  "longitude": 77.721663,
  "speed_kmph": 25.13,
  "heading": 233.14,
  "vehicle_status": "Moving",
  "ignition_status": "On",
  "signal_quality": "Good"
}
```

### Business use

- Track fleet location
- Monitor vehicle movement
- Detect low-signal vehicles
- Analyze routes and driving patterns
- Support ETA and delay prediction

---

## 3. `delivery_updates`

### Purpose

Carries delivery lifecycle events.

Typical event types include:

- Order picked up
- Dispatched
- Out for delivery
- Delivery delayed
- Delivered
- Delivery cancelled

### Example event

```json
{
  "event_id": "EVT000001",
  "order_id": "ORD000001",
  "driver_id": "DRV0019",
  "vehicle_id": "VEH0070",
  "event_timestamp": "2026-08-01 10:15:00",
  "region": "Vellore",
  "event_type": "Delivery Delay",
  "delivery_status": "Delayed",
  "delay_minutes": 18,
  "source": "simulated"
}
```

### Business use

- Track delivery lifecycle
- Detect delays in real time
- Calculate operational KPIs
- Trigger downstream alerts or processing

---

## 4. `traffic_updates`

### Purpose

Carries traffic conditions that may affect delivery routes.

### Example event

```json
{
  "timestamp": "2026-08-01 10:00:00",
  "location": "Chennai",
  "latitude": 13.0827,
  "longitude": 80.2707,
  "traffic_score": 72.5,
  "traffic_level": "Severe",
  "average_speed_kmph": 18.4
}
```

### Business use

- Identify congested areas
- Estimate route delays
- Improve route planning
- Support delivery-delay prediction

---

## 5. `weather_updates`

### Purpose

Carries weather information that can affect transportation and delivery.

### Example event

```json
{
  "date": "2026-08-01",
  "location": "Chennai",
  "latitude": 13.08,
  "longitude": 80.27,
  "temperature_c": 29.4,
  "humidity_pct": 81.2,
  "rainfall_mm": 14.7,
  "weather_condition": "Rain"
}
```

### Business use

- Detect weather-related delivery risks
- Correlate rainfall with delivery delays
- Support demand and delay prediction
- Improve operational planning

---

## Kafka Configuration

The project's Kafka connection and topic names are centralized in:

```text
ingestion/streaming/kafka_config.py
```

Environment variables are configured through:

```text
.env
```

Example:

```text
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

KAFKA_ORDERS_TOPIC=orders
KAFKA_GPS_TOPIC=gps_tracking
KAFKA_DELIVERY_TOPIC=delivery_updates
KAFKA_TRAFFIC_TOPIC=traffic_updates
KAFKA_WEATHER_TOPIC=weather_updates
```

## Message Flow

### Order flow

```text
orders.csv
    ↓
order_producer.py
    ↓
Kafka: orders
    ↓
order_consumer.py
    ↓
Spark / Processing
    ↓
Analytics
```

### GPS flow

```text
gps_tracking.csv
    ↓
gps_producer.py
    ↓
Kafka: gps_tracking
    ↓
gps_consumer.py
    ↓
Spark Structured Streaming
    ↓
Real-time fleet analytics
```

### External data flow

```text
Weather API ───────┐
                   ├──→ Kafka → Streaming Processing
Traffic API ───────┘
```

## Design Notes

- Topics are separated by event domain.
- JSON is used as the message format for this project prototype.
- `order_id` is used as the Kafka key for order events.
- GPS events can use `vehicle_id` as a natural partitioning key in a production implementation.
- The current project uses simulated streaming data where live production streams
  are unavailable.
- Kafka provides the event transport layer; Spark Structured Streaming can
  consume these topics for real-time processing.

## Interview Explanation

> "I separated Kafka topics by business event type. Orders and GPS tracking are
> the primary streaming streams, while delivery, traffic, and weather provide
> additional context. Producers publish JSON events to Kafka, consumers or
> Spark Structured Streaming process those events, and the processed data is
> then used for operational analytics, ML features, and dashboards."

