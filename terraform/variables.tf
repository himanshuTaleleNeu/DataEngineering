locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
  description = "Enter  Project ID"
}

variable "region" {
  description = "Region for GCP resources."
  default = "us-east1"
  type = string
}

variable "storage_class" {
  description = "Storage class type for bucket."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "trips_data_all"
}

variable "TABLE_NAME" {
  description = "BigQuery Table"
  type = string
  default = "ny_trips"
}