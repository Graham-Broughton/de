variable "project_id" {
    description = "Project ID"
    type        = string
    default     = "zoom-dataeng"
}

variable "region" {
    description = "Project region"
    type        = string
    default     = "us-central1"
}

variable "location" {
    description = "Project location"
    type        = string
    default     = "US"
}

variable "bq_dataset_name" {
    description = "The name of the BigQuery dataset"
    type        = string
    default     = "demo_dataset"
}

variable "gcs_storage_class" {
    description = "The storage class of the GCS bucket"
    type        = string
    default     = "STANDARD"
}

variable "gcs_bucket_name" {
    description = "The name of the GCS bucket"
    type        = string
    default     = "zoom-dataeng-demo-bucket"
}