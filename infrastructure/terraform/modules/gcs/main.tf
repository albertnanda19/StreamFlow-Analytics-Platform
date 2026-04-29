variable "project_id" { type = string }
variable "region"     { type = string }
variable "environment" { type = string }

resource "google_storage_bucket" "bronze" {
  name          = "${var.project_id}-streamflow-bronze-${var.environment}"
  location      = var.region
  force_destroy = var.environment != "prod"
  storage_class = "STANDARD"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition { age = 90 }
    action    { type = "SetStorageClass"; storage_class = "NEARLINE" }
  }
}

resource "google_storage_bucket" "silver" {
  name          = "${var.project_id}-streamflow-silver-${var.environment}"
  location      = var.region
  force_destroy = var.environment != "prod"
  storage_class = "STANDARD"

  versioning {
    enabled = true
  }
}

resource "google_storage_bucket" "gold" {
  name          = "${var.project_id}-streamflow-gold-${var.environment}"
  location      = var.region
  force_destroy = var.environment != "prod"
  storage_class = "STANDARD"

  versioning {
    enabled = true
  }
}

output "bronze_bucket_name" { value = google_storage_bucket.bronze.name }
output "silver_bucket_name" { value = google_storage_bucket.silver.name }
output "gold_bucket_name"   { value = google_storage_bucket.gold.name }
