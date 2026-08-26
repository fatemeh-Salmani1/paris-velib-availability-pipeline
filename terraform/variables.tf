variable "project_id" {
  description = "Google Cloud project ID"
  type        = string
}

variable "location" {
  description = "Location for GCS and BigQuery resources"
  type        = string
  default     = "EU"
}