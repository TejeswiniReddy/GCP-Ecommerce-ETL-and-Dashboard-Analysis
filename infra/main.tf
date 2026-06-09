# infra/main.tf
# Provisions all GCP resources for the retail ETL pipeline.
# Run: terraform apply -var="project_id=YOUR_PROJECT"

terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  backend "gcs" {
    bucket = "tf-state-retail-etl"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── GCS Buckets ─────────────────────────────────────────────────────────────

resource "google_storage_bucket" "retail_data" {
  name                        = "${var.project_id}-retail-data"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false

  lifecycle_rule {
    condition { age = 90 }
    action    { type = "SetStorageClass"; storage_class = "NEARLINE" }
  }
  lifecycle_rule {
    condition { age = 365 }
    action    { type = "SetStorageClass"; storage_class = "COLDLINE" }
  }

  versioning { enabled = true }

  labels = {
    env     = var.environment
    project = "retail-etl"
  }
}

resource "google_storage_bucket_object" "folder_raw" {
  name    = "raw/.keep"
  bucket  = google_storage_bucket.retail_data.name
  content = ""
}
resource "google_storage_bucket_object" "folder_staging" {
  name    = "staging/.keep"
  bucket  = google_storage_bucket.retail_data.name
  content = ""
}
resource "google_storage_bucket_object" "folder_dead_letter" {
  name    = "dead_letter/.keep"
  bucket  = google_storage_bucket.retail_data.name
  content = ""
}

# ── BigQuery Datasets ───────────────────────────────────────────────────────

resource "google_bigquery_dataset" "retail_staging" {
  dataset_id                 = "retail_staging"
  friendly_name              = "Retail Staging (Silver)"
  description                = "Cleaned, typed data from Dataflow ETL"
  location                   = var.bq_location
  delete_contents_on_destroy = false

  labels = { env = var.environment, layer = "silver" }
}

resource "google_bigquery_dataset" "retail_marts" {
  dataset_id                 = "retail_marts"
  friendly_name              = "Retail Marts (Gold)"
  description                = "Star schema and aggregated tables for dashboards"
  location                   = var.bq_location
  delete_contents_on_destroy = false

  labels = { env = var.environment, layer = "gold" }
}

# ── Service Account ──────────────────────────────────────────────────────────

resource "google_service_account" "etl_sa" {
  account_id   = "retail-etl-sa"
  display_name = "Retail ETL Service Account"
  description  = "Used by Dataflow, dbt, and Composer for retail pipeline"
}

resource "google_project_iam_member" "etl_dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.etl_sa.email}"
}
resource "google_project_iam_member" "etl_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.etl_sa.email}"
}
resource "google_project_iam_member" "etl_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.etl_sa.email}"
}
resource "google_project_iam_member" "etl_storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.etl_sa.email}"
}

# ── Cloud Composer (Airflow) ─────────────────────────────────────────────────

resource "google_composer_environment" "retail_composer" {
  name   = "retail-etl-composer"
  region = var.region

  config {
    software_config {
      image_version = "composer-2.5.0-airflow-2.6.3"
      python_version = "3"
      pypi_packages = {
        "apache-beam[gcp]" = "==2.54.0"
        "dbt-bigquery"     = "==1.7.0"
      }
    }
    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
        count      = 1
      }
      web_server {
        cpu       = 0.5
        memory_gb = 1.875
        storage_gb = 1
      }
      worker {
        cpu        = 2
        memory_gb  = 7.5
        storage_gb = 10
        min_count  = 1
        max_count  = 6
      }
    }
    environment_size = "ENVIRONMENT_SIZE_SMALL"
    node_config {
      service_account = google_service_account.etl_sa.email
    }
  }

  labels = { env = var.environment }
}
