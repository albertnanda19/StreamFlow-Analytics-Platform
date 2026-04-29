output "gcs_bronze_bucket" {
  description = "GCS Bronze layer bucket name"
  value       = module.gcs.bronze_bucket_name
}

output "gcs_silver_bucket" {
  description = "GCS Silver layer bucket name"
  value       = module.gcs.silver_bucket_name
}

output "gcs_gold_bucket" {
  description = "GCS Gold layer bucket name"
  value       = module.gcs.gold_bucket_name
}

output "gke_cluster_name" {
  description = "GKE cluster name"
  value       = module.gke.cluster_name
}

output "gke_cluster_endpoint" {
  description = "GKE cluster endpoint"
  value       = module.gke.cluster_endpoint
  sensitive   = true
}
