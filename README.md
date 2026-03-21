# 🍽️ Restaurant Data Platform

> Enterprise-grade data engineering platform for multi-location restaurant chains — built on **Snowflake**, **Apache Iceberg**, **Apache Airflow**, and **AWS IAM**.

[![CI](https://github.com/YOUR_USERNAME/restaurant-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/restaurant-data-platform/actions/workflows/ci.yml)
[![Terraform](https://github.com/YOUR_USERNAME/restaurant-data-platform/actions/workflows/terraform-apply.yml/badge.svg)](https://github.com/YOUR_USERNAME/restaurant-data-platform/actions/workflows/terraform-apply.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📐 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     EDGE INGESTION LAYER                         │
│   Toast/Square POS │ Kitchen IoT │ Inventory │ Delivery APIs     │
│           └──────────────┬──────────────┘                        │
│                   AWS IoT Core + Kafka (MSK)                     │
└──────────────────────────┬──────────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│              APACHE ICEBERG DATA LAKE (S3)                       │
│   Bronze (raw) → Silver (cleaned, PII-masked) → Gold (BI/ML)    │
│   • Time-travel  • Schema evolution  • ACID transactions         │
└──────────────────────────┬──────────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                       SNOWFLAKE                                   │
│   Corporate HQ │ Regional Ops │ Secure Data Sharing              │
│   • Dynamic Tables  • Row Access Policies  • Column Masking      │
└──────────────────────────┬──────────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│               APACHE AIRFLOW (Orchestration)                     │
│   Real-time (15min) │ Daily (6 AM) │ Weekly (ML retraining)      │
│   Astronomer / MWAA / EKS                                        │
└──────────────────────────┬──────────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│             IAM & SECURITY (PCI-DSS + SOC2)                      │
│   ABAC Franchisee Isolation │ Payment Tokenization │ Audit Trail  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Terraform 1.7+
- AWS CLI configured
- Snowflake account
- Docker (for local Airflow)

### 1. Install Dependencies

```bash
git clone https://github.com/YOUR_USERNAME/restaurant-data-platform.git
cd restaurant-data-platform
make install
```

### 2. Configure Environment

```bash
cp .env.example .env
# Fill in your Snowflake, AWS, and Airflow credentials
```

### 3. Bootstrap Infrastructure

```bash
make terraform-init ENV=dev
make terraform-plan ENV=dev
make terraform-apply ENV=dev
```

### 4. Initialize Snowflake

```bash
make setup-snowflake
```

### 5. Deploy Airflow DAGs

```bash
make deploy-dags ENV=dev
```

### 6. Run Tests

```bash
make test
```

---

## 📁 Project Structure

```
restaurant-data-platform/
├── .github/workflows/          # CI/CD pipelines
├── airflow/
│   ├── dags/                   # Pipeline orchestration
│   │   ├── restaurant_daily_operations.py
│   │   ├── real_time_pricing.py
│   │   ├── food_safety_monitoring.py
│   │   └── utils/              # Shared helpers
│   ├── plugins/operators/      # Custom Airflow operators
│   └── tests/                  # DAG integrity + operator tests
├── infrastructure/
│   └── terraform/
│       ├── modules/            # Reusable Terraform modules
│       │   ├── snowflake/      # Snowflake resources
│       │   ├── iceberg/        # S3 + Glue Catalog
│       │   ├── iam/            # Roles, policies, ABAC
│       │   └── airflow/        # MWAA / EKS setup
│       └── environments/       # dev / staging / prod
├── snowflake/
│   ├── schemas/                # Bronze / Silver / Gold SQL
│   ├── security/               # Row access + masking policies
│   └── procedures/             # Maintenance procedures
├── iceberg/
│   ├── schemas/                # Table definitions (YAML)
│   └── maintenance/            # Compaction + snapshot expiry
├── data-quality/
│   ├── great_expectations/     # GE expectations + checkpoints
│   └── soda/                   # Soda Core checks
├── monitoring/
│   ├── dashboards/             # Grafana dashboards (JSON)
│   └── alerts/                 # PagerDuty + Slack rules
├── docs/
│   ├── architecture.md
│   ├── runbooks/               # Incident response
│   └── onboarding/             # New franchisee setup
├── tests/
│   ├── integration/            # End-to-end tests
│   └── security/               # IAM policy tests
├── Makefile
├── pyproject.toml
└── .pre-commit-config.yaml
```

---

## 🏗️ Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Storage format** | Apache Iceberg | Time-travel for food safety audits, schema evolution |
| **Compute** | Snowflake | Multi-warehouse topology (kitchen vs analytics vs ML) |
| **Orchestration** | Airflow 2.8 | Data-aware scheduling, KubernetesPodOperator for ML |
| **Access control** | IAM ABAC + Snowflake RAP | Franchisee isolation without duplicating data |
| **Streaming** | Kafka (MSK) + micro-batch | Balance between latency and Iceberg write amplification |
| **PII handling** | Presidio + Snowflake masking | GDPR + PCI-DSS compliance at storage and query time |

---

## 🔐 Security Model

### Franchisee Isolation (ABAC)
Each franchisee can **only access their own location's data**, enforced at three layers:
1. **S3**: Object tags + IAM condition keys
2. **Snowflake**: Row Access Policies on every Gold table
3. **Airflow**: DAG-level parameter filtering

### PCI-DSS Compliance
- Payment card numbers **never stored** — Stripe/Braintree tokens only
- Dedicated KMS keys per location
- Immutable audit trail via Iceberg snapshots (7-year retention)

### Food Safety Compliance
- Temperature logs stored as **immutable Iceberg snapshots**
- Automatic PagerDuty alert if fridge > 40°F for > 2 hours
- 7-year retention policy enforced via S3 Object Lock

---

## 📊 Snowflake Warehouse Topology

| Warehouse | Size | Auto-suspend | Use Case |
|-----------|------|-------------|----------|
| `KITCHEN_ANALYTICS_WH` | XS | 1 min | Real-time kitchen operations |
| `BI_REPORTING_WH` | S | 5 min | Executive dashboards |
| `DATA_ENGINEERING_WH` | M | 10 min | ETL transformations |
| `ML_TRAINING_WH` | L | 30 min | Demand forecasting |

---

## 🧪 Testing

```bash
# Unit tests
make test

# DAG integrity (import + structure)
pytest airflow/tests/dags/ -v

# Integration tests (requires dev env)
pytest tests/integration/ -v --env=dev

# IAM policy tests
pytest tests/security/ -v
```

---

## 📈 Monitoring

Grafana dashboards available for:
- **Kitchen Efficiency** — prep times, equipment health, order throughput
- **Financial Overview** — COGS, P&L, delivery fee analysis
- **Data Pipeline Health** — DAG SLAs, Iceberg file metrics, Snowflake query performance

---

## 🤝 Contributing

1. Fork the repo and create a feature branch
2. Install pre-commit hooks: `make install`
3. Follow the [runbook](docs/runbooks/incident-response.md) for production changes
4. Open a PR against `develop` (never directly to `main`)

---

## 📄 License

MIT — see [LICENSE](LICENSE)
