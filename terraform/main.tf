terraform {
  required_version = ">= 1.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# -------------------
# GCS BUCKET
# -------------------

resource "google_storage_bucket" "rawg_bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  uniform_bucket_level_access = true
}

# -------------------
# BIGQUERY DATASETS
# -------------------

resource "google_bigquery_dataset" "raw" {
  dataset_id = var.bq_raw_dataset
  location   = var.location
}

resource "google_bigquery_dataset" "staging" {
  dataset_id = var.bq_staging_dataset
  location   = var.location
}

resource "google_bigquery_dataset" "marts" {
  dataset_id = var.bq_marts_dataset
  location   = var.location
}