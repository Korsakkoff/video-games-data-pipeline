output "bucket_name" {
  value = google_storage_bucket.rawg_bucket.name
}

output "datasets" {
  value = [
    google_bigquery_dataset.raw.dataset_id,
    google_bigquery_dataset.staging.dataset_id,
    google_bigquery_dataset.marts.dataset_id
  ]
}