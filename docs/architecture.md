# Architecture

## Overview

The **Restaurant Data Platform** is an enterprise-grade data engineering system designed for multi-location restaurant chains. It provides sub-minute operational intelligence, franchisee data isolation, and PCI-DSS/food-safety compliance.

---

## Data Flow

```
POS (Toast/Square)
Kitchen IoT Sensors       ──→  AWS IoT Core / Kafka (MSK)
Inventory Scanners                        │
Delivery Platforms                        ▼
                               Apache Iceberg (S3 + Glue)
                               ┌─────────────────────────┐
                               │  Bronze → Silver → Gold  │
                               └────────────┬────────────┘
                                            │
                               ┌────────────▼────────────┐
                               │        Snowflake         │
                               │  (Multi-warehouse topo)  │
                               └────────────┬────────────┘
                                            │
                               ┌────────────▼────────────┐
                               │    Apache Airflow        │
                               │  (Orchestration + SLA)   │
                               └─────────────────────────┘
```

---

## Layer Definitions

### Bronze (Raw)
- **Purpose**: Landing zone for all source data, exactly as received
- **Transformations**: Payment tokenisation only (PCI-DSS mandatory)
- **Retention**: 7 years for food safety tables; 90 days for others
- **Partitioning**: `location_id` (identity) + `date` (day transform)

### Silver (Cleaned)
- **Purpose**: Normalised, PII-anonymised, business-rule-applied data
- **Transformations**: PII anonymisation (Presidio), COGS calculation, inventory reconciliation
- **Partitioning**: `location_id` + `month`

### Gold (Business-Ready)
- **Purpose**: BI-facing aggregations and ML feature tables
- **Consumers**: Snowflake dynamic tables, Grafana dashboards, delivery platform APIs
- **Partitioning**: `location_id` (clustered)

---

## Snowflake Warehouse Topology

| Warehouse | Size | Auto-suspend | Purpose |
|-----------|------|-------------|---------|
| `KITCHEN_ANALYTICS_WH` | XS | 60s | Real-time kitchen ops, 15-min DAG |
| `BI_REPORTING_WH` | S | 5 min | Executive dashboards |
| `DATA_ENGINEERING_WH` | M | 10 min | ETL transformations |
| `ML_TRAINING_WH` | L | 30 min | Demand forecasting model training |

---

## IAM Security Model

### Five layers of protection

1. **PCI-DSS Payment Security** — raw PANs never touch the platform; Stripe/Braintree tokens only
2. **Network Segmentation** — POS → VPC endpoints; no public internet egress for sensors
3. **Data Access (ABAC)** — Franchisee isolation via S3 object tags + IAM condition keys + Snowflake Row Access Policies
4. **Audit & Compliance** — Iceberg immutable snapshots (Object Lock COMPLIANCE mode) + CloudTrail
5. **Secrets Rotation** — 90-day rotation via AWS Secrets Manager; RSA key pairs for Snowflake service accounts

### Role hierarchy

```
SYSADMIN
└── DATA_ENGINEER
└── CORPORATE_ANALYST
    ├── CORPORATE_FINANCE
    ├── CRM_ANALYST
    └── REGIONAL_MANAGER
        └── FRANCHISEE_OWNER
            └── FRANCHISEE_USER
KITCHEN_STAFF  (lateral — no access to financial data)
```

---

## Airflow DAG Overview

| DAG | Schedule | SLA | Description |
|-----|----------|-----|-------------|
| `restaurant_daily_operations` | `0 6 * * *` | 3 hours | Bronze→Gold full pipeline |
| `real_time_pricing` | `*/15 10-23 * * *` | 12 min | Surge pricing + 86-item management |
| `food_safety_monitoring` | `*/5 * * * *` | 4 min | Temperature violation alerting |

---

## Key Design Decisions

### Why Iceberg over Delta Lake?
- Open format — readable by Snowflake, Athena, EMR without vendor lock-in
- Superior time-travel for food safety audit investigations
- Schema evolution supports adding dietary tags without rewriting existing data

### Why micro-batch (not pure streaming) for kitchen IoT?
- Kafka consumers buffer 30 seconds of events before appending to Iceberg
- Reduces write amplification (Iceberg commit overhead per file)
- 30-second latency is acceptable for analytics; true real-time alerts go through IoT Core directly

### Why Snowflake Dynamic Tables over scheduled tasks?
- Automatic lag-based refresh triggers only when upstream data changes
- Eliminates complex dependency chaining for multi-hop transformations
- `kitchen_efficiency` refreshes every 5 minutes only when `orders_realtime` has new data
