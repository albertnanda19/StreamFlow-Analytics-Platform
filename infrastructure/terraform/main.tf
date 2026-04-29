terraform {
  required_version = ">= 1.6.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

module "gcs" {
  source      = "./modules/gcs"
  project_id  = var.project_id
  region      = var.region
  environment = var.environment
}

module "gke" {
  source      = "./modules/gke"
  project_id  = var.project_id
  region      = var.region
  environment = var.environment
  node_count  = var.gke_node_count
  machine_type = var.gke_machine_type
}
