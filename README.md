# Industrial Energy Streaming Data Platform

A near-real-time industrial telemetry data platform built with **AWS, Databricks, PySpark, Delta Lake, Unity Catalog, and GitHub Actions**.

The project simulates industrial sensor telemetry from assets such as pumps, motors, turbines, pipelines, and storage equipment. Telemetry is published through **AWS IoT Core**, routed through **Amazon Kinesis Data Streams**, processed using **Databricks Structured Streaming**, and transformed through a **Bronze → Silver → Quarantine → Gold** architecture.

The platform includes event-time processing, deduplication, data-quality validation, anomaly classification, asset-health metrics, orchestration, governance, monitoring, CI/CD, and an operational dashboard.

---

## 1. Project Overview

Industrial equipment continuously generates telemetry such as:

* Temperature
* Pressure
* Vibration
* Flow rate
* Power consumption
* Equipment status

The objective of this project is to demonstrate how an industrial organization could transform streaming sensor events into reliable operational information for monitoring asset health and abnormal conditions.

The simulated environment intentionally introduces data-quality scenarios including:

* Missing measurements
* Invalid measurements
* Duplicate events
* Late-arriving events
* Classified operational anomalies

The resulting Gold datasets support an operational monitoring dashboard containing asset-health status, anomaly summaries, energy consumption, temperature, vibration, and asset-level metrics.

### Key engineering capabilities demonstrated

* AWS IoT Core and MQTT ingestion
* Amazon Kinesis Data Streams
* Databricks Structured Streaming
* PySpark
* Delta Lake
* Medallion architecture
* Event-time processing
* Watermarking
* Streaming deduplication
* Data-quality validation
* Quarantine processing
* Unity Catalog governance
* Lakeflow Jobs orchestration
* Databricks Asset Bundles
* Git/GitHub
* GitHub Actions
* OIDC-based authentication
* CloudWatch monitoring
* Databricks SQL visualization

---

# 2. Architecture

```text
                         INDUSTRIAL ENERGY STREAMING PLATFORM
┌───────────────────────────────────────────────────────────────────────────────┐
│                                                                               │
│  Python Industrial Sensor Simulator                                           │
│                                                                               │
│  Pumps | Motors | Turbines | Pipelines | Storage                              │
│                                                                               │
│  Temperature | Pressure | Vibration | Flow | Power | Status                  │
└───────────────────────────────┬───────────────────────────────────────────────┘
                                │
                                │ MQTT over TLS
                                ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                           AWS IoT Core                                        │
│                                                                               │
│  X.509 certificate authentication                                             │
│  MQTT topic: industrial/telemetry                                             │
│  IoT Rule: industrial-telemetry-to-kinesis                                    │
└───────────────────────────────┬───────────────────────────────────────────────┘
                                │
                                │ Kinesis PutRecord
                                ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                       Amazon Kinesis Data Streams                              │
│                                                                               │
│  Stream: industrial-telemetry                                                │
│  Partition key: asset_id                                                     │
└───────────────────────────────┬───────────────────────────────────────────────┘
                                │
                                │ Streaming consumption
                                ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                         DATABRICKS                                             │
│                                                                               │
│  Databricks Structured Streaming                                               │
│             │                                                                 │
│             ▼                                                                 │
│       ┌─────────────┐                                                         │
│       │   BRONZE    │  Raw telemetry + Kinesis metadata                      │
│       └──────┬──────┘                                                         │
│              │                                                                │
│              ▼                                                                │
│       ┌─────────────┐                                                         │
│       │   SILVER    │  Validation + watermark + deduplication                │
│       └──────┬──────┘                                                         │
│              │                                                                │
│       ┌──────┴───────────────┐                                                │
│       │                      │                                                │
│       ▼                      ▼                                                │
│   VALID DATA            QUARANTINE                                            │
│       │                 Invalid records                                       │
│       │                                                                         │
│       └──────────────┬───────────────┘                                        │
│                      ▼                                                        │
│               ┌─────────────┐                                                 │
│               │    GOLD     │  Asset health + anomaly summaries              │
│               └──────┬──────┘                                                 │
│                      │                                                        │
└──────────────────────┼────────────────────────────────────────────────────────┘
                       │
                       ▼
             ┌─────────────────────┐
             │ Databricks SQL      │
             │ Operational         │
             │ Monitoring Dashboard│
             └─────────────────────┘


         CROSS-CUTTING PLATFORM SERVICES
         
         Unity Catalog → Governance
         Lakeflow Jobs → Orchestration
         CloudWatch → AWS Monitoring
         Git/GitHub → Version Control
         DAB → Deployment as Code
         GitHub Actions → CI/CD
         OIDC → Authentication
```

---

# 3. Project Workflow

## Step 1 — Sensor Simulation

Python generates industrial telemetry representing connected assets.

Each event contains operational measurements and metadata.

The simulator also introduces controlled data-quality scenarios such as:

* Duplicate events
* Missing measurements
* Invalid measurements
* Late events
* Classified anomalies

---

## Step 2 — IoT Ingestion

Telemetry is published to **AWS IoT Core** using MQTT over TLS with an X.509 client certificate.

MQTT topic:

```text
industrial/telemetry
```

AWS IoT Core receives the telemetry and applies an IoT Rule that forwards messages to Kinesis.

---

## Step 3 — Streaming Ingestion

Amazon Kinesis Data Streams provides the streaming ingestion layer.

```text
AWS IoT Core
      │
      ▼
Kinesis: industrial-telemetry
```

The simulator uses the asset ID as the Kinesis partition key.

This provides partition-based distribution while preserving ordering semantics within a partition.

Databricks consumes the stream using a Databricks service credential configured to assume the required AWS IAM role.

---

## Step 4 — Bronze Layer

Raw incoming telemetry is persisted into Delta format.

Bronze preserves:

* Telemetry content
* Kinesis metadata
* Partition information
* Sequence number
* Kinesis arrival timestamp
* Ingestion timestamp

Bronze is append-oriented and uses Databricks Structured Streaming.

Location:

```text
/Volumes/industrial_sensor/sensor_data/industrial_sensor_data/delta/bronze_telemetry
```

---

## Step 5 — Silver Layer

Silver transforms the Bronze stream into validated analytical data.

The processing includes:

### Event-time processing

The telemetry event timestamp is converted into a timestamp column.

A 10-minute event-time watermark is applied to bound streaming state.

### Deduplication

Events are deduplicated using:

```text
event_id
```

This prevents duplicate events from propagating into the valid Silver dataset.

### Data-quality validation

The pipeline validates:

* Event identity
* Site ID
* Asset ID
* Asset type
* Measurement completeness
* Measurement ranges
* Event timestamp validity

Records failing validation are classified as:

```text
INVALID
```

Valid records are classified as:

```text
VALID
```

---

# 4. Quarantine Layer

Invalid records are separated from valid Silver data instead of being discarded.

Examples include:

* Missing measurements
* Invalid measurement ranges
* Invalid identity fields
* Invalid timestamps

Quarantine location:

```text
/Volumes/industrial_sensor/sensor_data/industrial_sensor_data/delta/quarantine_telemetry
```

This preserves problematic records for investigation and data-quality analysis.

---

# 5. Gold Layer

The Gold layer provides operationally useful aggregated datasets.

## Gold Asset Health

The asset-health dataset calculates metrics such as:

* Average temperature
* Maximum temperature
* Average pressure
* Maximum pressure
* Average vibration
* Maximum vibration
* Average flow rate
* Average power consumption
* Telemetry count
* Anomaly count
* Last event timestamp

A rule-based health status is then assigned:

```text
anomaly_count > 0  → ATTENTION
anomaly_count = 0  → NORMAL
```

This is an **operational rule**, not an ML-based predictive-maintenance model.

---

## Gold Anomaly Summary

The anomaly summary aggregates classified telemetry anomalies by:

```text
site_id
asset_id
asset_type
anomaly_type
```

It also records:

```text
anomaly_count
last_anomaly_ts
```

The current implementation summarizes classified anomalies generated by the telemetry source; it does not perform statistical or machine-learning anomaly detection.

---

# 6. AWS Components

| Component                   | Purpose                        |
| --------------------------- | ------------------------------ |
| AWS IoT Core                | MQTT telemetry ingestion       |
| X.509 Certificate           | Device authentication          |
| IoT Rule                    | Routes telemetry to Kinesis    |
| Amazon Kinesis Data Streams | Streaming event transport      |
| AWS IAM                     | Controls AWS service access    |
| Amazon CloudWatch           | Kinesis operational monitoring |

### AWS access paths

There are two main AWS access paths:

```text
AWS IoT Core
      │
      ▼
industrial-iot-kinesis-role
      │
      ▼
Kinesis PutRecord
```

and:

```text
Databricks
      │
      ▼
industrial-kinesis-databricks-role
      │
      ▼
Kinesis Read
```

The configured IAM permissions are scoped to the operations required by these integration paths.

---

# 7. Databricks Components

| Component                       | Purpose                                |
| ------------------------------- | -------------------------------------- |
| Databricks Structured Streaming | Streaming ingestion and transformation |
| PySpark                         | Data processing                        |
| Delta Lake                      | Reliable lakehouse storage             |
| Unity Catalog                   | Governance of managed storage          |
| Lakeflow Jobs                   | Pipeline orchestration                 |
| Databricks SQL                  | Operational visualization              |
| Databricks Asset Bundles        | Deployment configuration as code       |

---

# 8. Data Model

The telemetry event follows this logical model:

```text
TelemetryEvent
│
├── event_id
├── site_id
├── asset_id
├── asset_type
├── event_timestamp
├── ingestion_timestamp
│
├── temperature
├── pressure
├── vibration
├── flow_rate
├── power_kw
│
├── status
└── anomaly_type
```

### Asset types

The simulated environment includes:

```text
pump
motor
turbine
pipeline
storage
```

### Conceptual relationships

```text
SITE
 │
 └── ASSET
       │
       └── TELEMETRY EVENT
              │
              ├── Measurements
              ├── Status
              └── Anomaly classification
```

---

# 9. Sample Telemetry

Example telemetry event:

```json
{
  "event_id": "evt-001",
  "site_id": "SITE-001",
  "asset_id": "PUMP-001",
  "asset_type": "pump",
  "event_timestamp": "2026-10-01T14:10:56.621Z",
  "ingestion_timestamp": "2026-10-01T14:11:02.100Z",
  "temperature": 72.4,
  "pressure": 31.8,
  "vibration": 2.1,
  "flow_rate": 145.6,
  "power_kw": 82.5,
  "status": "RUNNING",
  "anomaly_type": null
}
```

Example anomalous event:

```json
{
  "event_id": "evt-002",
  "site_id": "SITE-001",
  "asset_id": "MOTOR-001",
  "asset_type": "motor",
  "event_timestamp": "2026-10-01T14:10:56.621Z",
  "ingestion_timestamp": "2026-10-01T14:11:02.100Z",
  "temperature": 76.2,
  "pressure": 28.4,
  "vibration": 7.8,
  "flow_rate": 120.3,
  "power_kw": 94.7,
  "status": "RUNNING",
  "anomaly_type": "HIGH_VIBRATION"
}
```

---

# 10. Data Quality Strategy

The project intentionally demonstrates common streaming data-quality challenges.

| Data issue          | Handling                                       |
| ------------------- | ---------------------------------------------- |
| Duplicate event     | Deduplicated using `event_id`                  |
| Missing measurement | Classified as invalid                          |
| Invalid measurement | Classified as invalid                          |
| Invalid identity    | Classified as invalid                          |
| Invalid timestamp   | Classified as invalid                          |
| Late-arriving event | Event-time watermarking bounds streaming state |
| Invalid record      | Routed to quarantine                           |

The pipeline does not automatically repair or impute invalid measurements.

---

# 11. Orchestration

Lakeflow Jobs orchestrates the processing pipeline through a dependency-based workflow:

```text
bronze_ingestion
       │
       ▼
silver_transform
       │
       ▼
silver_persistence
       │
       ▼
gold
```

Tasks execute only after successful upstream dependencies.

Configured reliability features include:

* Up to 3 automatic retries per task
* Failure email notification
* Multi-task dependency management
* Run history
* Task-level execution status

Automatic retries provide basic resilience against transient failures but are not a guarantee against all failure scenarios.

---

# 12. Data Governance

The project uses a Unity Catalog managed Volume as the governed storage boundary.

The Volume contains:

```text
industrial_sensor
└── sensor_data
    └── industrial_sensor_data
        ├── bronze_telemetry
        ├── silver_telemetry
        ├── quarantine_telemetry
        ├── gold_asset_health
        └── gold_anomaly_summary
```

The managed Volume provides a centralized governed storage location for the pipeline's Delta datasets.

The current implementation does not claim full Unity Catalog table-level lineage for these Volume-contained Delta datasets.

---

# 13. Monitoring & Observability

## AWS CloudWatch

CloudWatch was used to inspect Kinesis operational metrics including:

* Incoming records
* Incoming bytes
* Successful writes
* Write latency
* Successful reads
* Read latency
* Records consumed
* Bytes consumed
* Consumer iterator age
* Throughput exceptions

Consumer iterator age was specifically inspected to demonstrate visibility into Kinesis consumer backlog.

Automated CloudWatch alarms were not configured in this portfolio implementation.

---

## Databricks Monitoring

Databricks provides operational visibility through:

* Lakeflow Job history
* Task execution status
* Retry information
* Delta transaction history
* Job/run metadata
* Dashboard results

Delta history also provides traceability for changes to the Gold datasets.

---

# 14. CI/CD

The project uses:

```text
GitHub
   │
   ▼
GitHub Actions
   │
   ├── Validate
   │
   ▼
Databricks CLI
   │
   ▼
Databricks Asset Bundle
   │
   ▼
Databricks Job
```

### Pull requests and changes

GitHub Actions validates the Databricks Asset Bundle.

### Main branch

A push to `main` triggers:

```text
Validation
    ↓
Deployment
```

### Authentication

GitHub Actions authenticates to Databricks using:

```text
GitHub OIDC
      ↓
Databricks Service Principal
      ↓
Databricks CLI
```

This avoids storing a long-lived Databricks Personal Access Token for CI/CD authentication.

---

# 15. Implementation Status

| Capability                        | Status     |
| --------------------------------- | ---------- |
| Python sensor simulator           | ✅ Complete |
| Industrial telemetry generation   | ✅ Complete |
| MQTT telemetry publishing         | ✅ Complete |
| AWS IoT Core                      | ✅ Complete |
| X.509 device authentication       | ✅ Complete |
| IoT → Kinesis routing             | ✅ Complete |
| Amazon Kinesis Data Streams       | ✅ Complete |
| Databricks Kinesis ingestion      | ✅ Complete |
| Bronze Delta layer                | ✅ Complete |
| Silver transformation             | ✅ Complete |
| Event-time watermarking           | ✅ Complete |
| Event deduplication               | ✅ Complete |
| Data-quality validation           | ✅ Complete |
| Quarantine layer                  | ✅ Complete |
| Gold asset-health metrics         | ✅ Complete |
| Gold anomaly summary              | ✅ Complete |
| Lakeflow Jobs orchestration       | ✅ Complete |
| Task retries                      | ✅ Complete |
| Failure email notification        | ✅ Complete |
| Unity Catalog managed Volume      | ✅ Complete |
| Databricks Asset Bundle           | ✅ Complete |
| Git/GitHub repository             | ✅ Complete |
| GitHub Actions validation         | ✅ Complete |
| GitHub Actions deployment         | ✅ Complete |
| GitHub OIDC authentication        | ✅ Complete |
| CloudWatch metric validation      | ✅ Complete |
| Databricks SQL dashboard          | ✅ Complete |
| Automated CloudWatch alarms       | 🔜 Future  |
| ML predictive maintenance         | 🔜 Future  |
| Production RBAC model             | 🔜 Future  |
| Automated data-quality monitoring | 🔜 Future  |
| Production environment separation | 🔜 Future  |
| Automated cost budgets/alerts     | 🔜 Future  |

---

# 16. Validation Results

The completed implementation was validated across the major architecture layers.

### Data validation snapshot

```text
Bronze records                  26
Silver valid records             9
Quarantine invalid records       5
Gold Asset Health records        5
Gold Anomaly Summary records     3
```

The counts intentionally differ because each layer has a different purpose.

### Silver quality validation

```text
VALID    9
```

No duplicate `event_id` values were present in the validated Silver dataset.

### Quarantine validation

```text
INVALID    5
```

### Gold Asset Health

```text
MOTOR-001   motor      ATTENTION   telemetry=2   anomalies=1
PIPE-001    pipeline   ATTENTION   telemetry=3   anomalies=1
PUMP-001    pump       NORMAL      telemetry=2   anomalies=0
STOR-001    storage    ATTENTION   telemetry=1   anomalies=1
TURB-001    turbine    NORMAL      telemetry=1   anomalies=0
```

### Gold Anomaly Summary

```text
MOTOR-001   LATE_EVENT        1
PIPE-001    HIGH_TEMPERATURE  1
STOR-001    HIGH_VIBRATION    1
```

These values represent a validation snapshot from the simulated environment and should not be interpreted as permanent production fleet statistics.

---

# 17. Validation Checklist

```text
[x] AWS IoT Core → Kinesis ingestion
[x] Kinesis → Databricks streaming ingestion
[x] Bronze streaming persistence
[x] Silver streaming transformation
[x] Event-time watermarking
[x] Event deduplication
[x] Data-quality validation
[x] Quarantine processing
[x] Gold asset-health aggregation
[x] Gold anomaly aggregation
[x] Lakeflow Jobs DAG
[x] Task retry configuration
[x] Failure notifications
[x] Unity Catalog managed Volume
[x] Databricks Asset Bundle validation
[x] Git repository validation
[x] GitHub Actions validation
[x] GitHub Actions deployment
[x] GitHub OIDC authentication
[x] Databricks SQL dashboard
```

The implementation was validated through existing successful executions, streaming history, Delta history, data-quality checks, orchestration results, CI/CD results, and dashboard results.

A final live end-to-end execution was intentionally not repeated after validation in order to avoid unnecessary cloud usage and cost.

---

# 18. Cost & Resource Management

This project uses usage-based AWS and Databricks services for streaming ingestion, processing, orchestration, monitoring, and visualization.

During development, cloud resources were used for controlled validation rather than continuous production operation.

After validation, unnecessary workloads are stopped to avoid ongoing charges.

A production deployment would require workload-specific:

* Compute sizing
* Scheduling
* Autoscaling policies
* Data retention policies
* Monitoring thresholds
* Budget controls
* Cost alerts

These production cost controls are not claimed as implemented in the portfolio environment.

---

# 19. Key Engineering Decisions

## Preserve raw telemetry

Bronze retains incoming telemetry and streaming metadata so that downstream processing does not destroy the original analytical context.

## Validate before analytics

Silver separates valid and invalid data before downstream aggregation.

## Quarantine instead of discard

Invalid records are preserved for investigation instead of being silently dropped.

## Deduplicate using event identity

`event_id` provides a simple business-level identity for preventing duplicate events from propagating into valid Silver data.

## Use event-time processing

Watermarking allows the streaming pipeline to reason about event timestamps while bounding state.

## Keep Gold operational

Gold provides current aggregated asset-health and anomaly information for operational analytics.

## Use OIDC for CI/CD

GitHub Actions uses OIDC federation rather than storing a long-lived Databricks PAT.

---

# 20. Project Structure

```text
industrial-energy-streaming/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── resources/
│   └── industrial_energy_job.yml
│
├── src/
│   └── notebooks/
│       ├── Step 4 - Build the Bronze layer.py
│       ├── Step 5 - Silver Layer.py
│       ├── Step 6 - Write to Silver Layer persistance.py
│       └── Step 7 - Build the Gold Layer.py
│
├── databricks.yml
├── .gitignore
└── README.md
```

The repository contains the Databricks Asset Bundle configuration, deployment resources, processing notebooks, and CI/CD workflow.

---

# 21. Future Improvements

The following capabilities would be appropriate for a production-oriented evolution of the platform:

* Implement CloudWatch alarms for Kinesis consumer lag and throughput exceptions.
* Add automated data-quality metrics and historical quality dashboards.
* Introduce stronger schema evolution controls.
* Add automated unit and integration tests to the CI pipeline.
* Tighten service-principal permissions according to least-privilege requirements.
* Add explicit development, test, and production environments.
* Add production-grade RBAC and group-based access controls.
* Introduce predictive-maintenance ML models using historical telemetry.
* Add feature engineering and model monitoring.
* Implement automated AWS and Databricks cost budgets and alerts.
* Evaluate table-level Unity Catalog registration where the storage architecture supports it.
* Integrate operational alerting with an incident-management platform.

---

# 22. Interview Discussion Points

This project can be discussed through several Data Engineering themes.

### Streaming

**Question:** Why Kinesis?

**Answer:** Kinesis provides a managed streaming transport layer between the IoT ingestion system and Databricks, allowing telemetry to be processed as streaming events rather than relying exclusively on batch file ingestion.

### Data quality

**Question:** What happens to invalid records?

**Answer:** Silver applies explicit identity, completeness, measurement-range, and timestamp validation rules. Invalid records are classified and written to a quarantine dataset instead of being silently discarded.

### Deduplication

**Question:** How do you handle duplicate events?

**Answer:** The pipeline uses `event_id` as the deduplication key in Silver.

### Late events

**Question:** How do you handle late-arriving data?

**Answer:** The pipeline applies event-time processing with a 10-minute watermark to bound streaming state. The watermark should not be interpreted as a guarantee that events arriving later than 10 minutes are always rejected.

### Governance

**Question:** How is the data governed?

**Answer:** Unity Catalog provides the governed storage boundary through a managed Volume containing the Bronze, Silver, Quarantine, and Gold Delta datasets.

### CI/CD

**Question:** How is deployment automated?

**Answer:** The Databricks job is defined using Databricks Asset Bundles. GitHub Actions validates the bundle and deploys it on changes to the main branch. Authentication uses GitHub OIDC with a Databricks service principal instead of a long-lived PAT.

### Production readiness

**Question:** Is this production-ready?

**Answer:** The project demonstrates the core architecture and engineering patterns, but it is a portfolio implementation. Production deployment would require additional controls such as stronger RBAC, automated alerting, environment separation, production data-quality monitoring, cost controls, disaster recovery, and predictive-maintenance modeling.

---

# 23. Conclusion

This project demonstrates an end-to-end approach to building a near-real-time industrial data platform using AWS and Databricks.

It combines:

```text
IoT ingestion
      ↓
Streaming transport
      ↓
Lakehouse processing
      ↓
Data quality
      ↓
Governed storage
      ↓
Operational analytics
      ↓
Orchestration
      ↓
Monitoring
      ↓
CI/CD
```

The implementation focuses on practical Data Engineering concerns rather than only demonstrating individual technologies: reliable ingestion, event-time processing, deduplication, data-quality handling, quarantine, orchestration, governance, deployment automation, monitoring, and cost awareness.

The platform provides a foundation that could be extended toward production industrial analytics and predictive-maintenance use cases.
