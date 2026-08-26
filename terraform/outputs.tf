output "data_lake_bucket_name" {
  description = "Name of the GCS data-lake bucket"
  value       = google_storage_bucket.data_lake.name
}

output "raw_dataset_id" {
  description = "BigQuery raw dataset ID"
  value       = google_bigquery_dataset.raw.dataset_id
}

output "analytics_dataset_id" {
  description = "BigQuery analytics dataset ID"
  value       = google_bigquery_dataset.analytics.dataset_id
}