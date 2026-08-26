provider "google" {
  project = var.project_id
}

locals {
  common_labels = {
    project     = "paris-velib"
    environment = "portfolio"
    managed_by  = "terraform"
  }
}

resource "google_project_service" "apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "storage.googleapis.com",
  ])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_storage_bucket" "data_lake" {
  name     = "${var.project_id}-data-lake"
  project  = var.project_id
  location = var.location

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  labels = local.common_labels

  depends_on = [google_project_service.apis]
}

resource "google_bigquery_dataset" "raw" {
  dataset_id = "velib_raw"
  project    = var.project_id
  location   = var.location

  description                = "Raw Vélib station availability data"
  delete_contents_on_destroy = false

  labels = local.common_labels

  depends_on = [google_project_service.apis]
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id = "velib_analytics"
  project    = var.project_id
  location   = var.location

  description                = "Transformed Vélib data prepared for analytics and dashboards"
  delete_contents_on_destroy = false

  labels = local.common_labels

  depends_on = [google_project_service.apis]
}