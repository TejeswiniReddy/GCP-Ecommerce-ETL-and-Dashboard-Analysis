# GCP E-Commerce Retail Analytics Pipeline

A production-grade end-to-end data engineering pipeline on **Google Cloud Platform** that ingests raw e-commerce transactional data, transforms it through a **Medallion Architecture** (Bronze → Silver → Gold), and serves a **Looker Studio / BigQuery** dashboard for retail business intelligence.

---

## Architecture Overview

```
Raw Data Sources
(CSV / Pub/Sub / API)
        │
        ▼
  Cloud Storage (GCS)          ← Bronze Layer (raw landing zone)
        │
        ▼
  Cloud Dataflow (Apache Beam)  ← ETL: clean, validate, deduplicate
        │
        ▼
  BigQuery Staging Dataset      ← Silver Layer (cleaned, typed)
        │
        ▼
  dbt Transformations           ← Business logic, dimensional models
        │
        ▼
  BigQuery Marts Dataset        ← Gold Layer (star schema, aggregates)
        │
        ▼
  Looker Studio Dashboard       ← Revenue, Orders, Customer, Product KPIs
```

**Orchestration:** Cloud Composer (Apache Airflow)  
**Infrastructure:** Terraform  
**CI/CD:** GitHub Actions  
**Testing:** pytest + dbt tests  

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud Provider | Google Cloud Platform (GCP) |
| Object Storage | Cloud Storage (GCS) |
| ETL / Streaming | Cloud Dataflow (Apache Beam) |
| Data Warehouse | BigQuery |
| Transformation | dbt (data build tool) |
| Orchestration | Cloud Composer (Airflow) |
| IaC | Terraform |
| Dashboard | Looker Studio (connected to BigQuery) |
| Language | Python 3.11 |
| CI/CD | GitHub Actions |

---

## Dataset: Retail E-Commerce

Simulated dataset modelled after real retail transaction systems with:

| Table | Rows (simulated) | Description |
|---|---|---|
| `orders` | ~500K | Order header records |
| `order_items` | ~1.5M | Line-item level detail |
| `customers` | ~100K | Customer master data |
| `products` | ~10K | Product catalog |
| `returns` | ~50K | Return / refund events |

---

## Project Structure

```
gcp_ecommerce_etl/
├── data/                        # Sample data generator
│   └── generate_sample_data.py
├── etl/                         # Dataflow pipeline (Apache Beam)
│   ├── pipeline.py              # Main Beam pipeline
│   ├── transforms.py            # Custom PTransforms
│   └── schema.py                # BigQuery schemas
├── dbt_transforms/              # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   └── models/
│       ├── staging/             # Silver layer: clean + type raw data
│       ├── intermediate/        # Business logic joins
│       └── marts/               # Gold layer: star schema + aggregates
├── dashboard/
│   └── looker_studio_setup.md   # Dashboard setup guide + SQL queries
├── infra/                       # Terraform IaC
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── dags/                        # Airflow DAG
│   └── retail_etl_dag.py
├── tests/                       # Unit + integration tests
│   ├── test_transforms.py
│   └── test_data_quality.py
├── .github/workflows/
│   └── ci.yml                   # GitHub Actions CI
├── requirements.txt
└── README.md
```

---

## Quick Start

### 1. Prerequisites

```bash
# GCP project with billing enabled
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID

# Python environment
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Provision Infrastructure (Terraform)

```bash
cd infra/
terraform init
terraform plan -var="project_id=YOUR_PROJECT_ID"
terraform apply -var="project_id=YOUR_PROJECT_ID"
```

### 3. Generate and Upload Sample Data

```bash
python data/generate_sample_data.py --rows 100000 --output-dir ./data/raw/
gsutil -m cp data/raw/*.csv gs://YOUR_BUCKET/raw/
```

### 4. Run the Dataflow ETL Pipeline

```bash
python etl/pipeline.py \
  --project YOUR_PROJECT_ID \
  --region us-central1 \
  --input gs://YOUR_BUCKET/raw/ \
  --staging_location gs://YOUR_BUCKET/staging/ \
  --temp_location gs://YOUR_BUCKET/temp/ \
  --runner DataflowRunner
```

### 5. Run dbt Transformations

```bash
cd dbt_transforms/
dbt deps
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### 6. Trigger via Airflow

Upload the DAG to Cloud Composer and trigger `retail_etl_pipeline` from the Airflow UI, or:

```bash
gcloud composer environments run YOUR_COMPOSER_ENV \
  --location us-central1 \
  dags trigger -- retail_etl_pipeline
```

---

## Dashboard KPIs

The Looker Studio dashboard (connected live to BigQuery Gold layer) tracks:

- **Revenue Analytics** — GMV, AOV, revenue by category, MoM growth
- **Order Performance** — Order volume, fulfillment rate, cancellation rate
- **Customer Intelligence** — New vs returning, LTV cohorts, churn signals
- **Product Insights** — Top SKUs, return rates, attach rates
- **Geographic Revenue** — State/region heatmap

---

## Data Quality Checks

dbt tests enforce:
- No null order IDs or customer IDs
- Revenue values > 0
- Order dates not in the future
- Referential integrity: every order item has a valid order and product
- Return amount never exceeds original order amount

---

## Author

**Tejeswini Reddy** | [linkedin.com/in/tejeswini-reddy](https://linkedin.com/in/tejeswini-reddy) | [github.com/TejeswiniReddy](https://github.com/TejeswiniReddy)
