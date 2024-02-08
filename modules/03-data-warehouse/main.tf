variable "service_account_key" {
  description = "My Credentials"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}

variable "project_id" {
  description = "Project id"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
}

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  credentials = file(var.service_account_key)
  project     = var.project_id
  region      = var.region
}

resource "random_id" "bucket" {
  byte_length = 8
}

resource "google_storage_bucket" "gcs-bucket" {
  # name          = var.gcs_bucket_name
  name          = "module-3-bucket-${random_id.bucket.hex}"
  location      = var.location
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
