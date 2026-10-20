# Project: Real-Time Restaurant Order Analytics

## 1. Business Problem

A food delivery platform receives thousands of order events from restaurants.

Traditional batch reporting cannot provide sufficiently up-to-date information about restaurant demand.

The business needs to know:

**How many confirmed orders has each restaurant received in the last 20 minutes?**

This metric can help downstream systems understand restaurant demand and support delivery-time prediction.

## 2. Project Objective

Build a streaming data engineering pipeline that:

1. Generates simulated restaurant order events.
2. Publishes events into Apache Kafka.
3. Ingests events using Databricks Structured Streaming.
4. Stores raw data in Bronze Delta tables.
5. Validates and deduplicates data in Silver.
6. Calculates restaurant order metrics in Gold.
7. Executes through scheduled Databricks Jobs.
8. Supports version control, CI/CD, monitoring and recovery.

## 3. Technology Stack

- **Development:** PyCharm, Python
- **Version Control:** Git, GitHub
- **CI/CD:** GitHub Actions
- **Event Streaming:** Confluent Cloud Kafka
- **Processing:** Databricks, PySpark Structured Streaming
- **Storage:** Delta Lake
- **Orchestration:** Databricks Jobs

## 4. Data Requirements

Each simulated event will contain:

| Field | Meaning |
|---|---|
| event_id | Unique event identifier |
| order_id | Unique order identifier |
| restaurant_id | Restaurant identifier |
| event_type | Order lifecycle event |
| event_time | Business event timestamp |

## 5. Success Criteria

- Events can be published to Kafka and verified.
- Raw events are ingested without losing required metadata.
- Invalid events are identified and handled.
- Duplicate confirmed-order events do not inflate counts.
- Gold provides correct 20-minute restaurant-level counts.
- The pipeline can restart and process data correctly.
- Automated tests run through GitHub Actions.
- Scheduled processing and run history are demonstrated.

## 6. Scope and Limitations

This is an educational recreation inspired by DoorDash Engineering, not an implementation of DoorDash's internal code.

Direct Kafka access, automated deployment and execution on Databricks Free Edition must be validated before they are considered supported.

## 7. Primary Industry Reference

DoorDash Engineering — *Supercharging DoorDash's Marketplace Decision-Making with Real-Time Knowledge* (March 31, 2020).

https://careersatdoordash.com/blog/supercharging-doordashs-marketplace-decision-making-with-real-time-knowledge/