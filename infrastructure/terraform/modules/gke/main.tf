variable "project_id"   { type = string }
variable "region"       { type = string }
variable "environment"  { type = string }
variable "node_count"   { type = number; default = 2 }
variable "machine_type" { type = string; default = "n2-standard-4" }

resource "google_container_cluster" "streamflow" {
  name     = "streamflow-${var.environment}"
  location = var.region
  project  = var.project_id

  remove_default_node_pool = true
  initial_node_count       = 1

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }
}

resource "google_container_node_pool" "primary" {
  name       = "streamflow-primary-pool"
  location   = var.region
  cluster    = google_container_cluster.streamflow.name
  project    = var.project_id
  node_count = var.node_count

  node_config {
    machine_type = var.machine_type
    disk_size_gb = 100
    disk_type    = "pd-ssd"
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }
}

output "cluster_name"     { value = google_container_cluster.streamflow.name }
output "cluster_endpoint" { value = google_container_cluster.streamflow.endpoint }
