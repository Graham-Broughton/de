terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
    project = "zoom-dataeng"
    region  = "us-central1"
  
}

resource "google_storage_bucket" "demo-bucket" {
  name    = "zoom-dataeng-demo-bucket"
  location = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}