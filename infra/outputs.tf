output "state_bucket" {
  description = "Terraform state bucket. Goes in the backend block in versions.tf."
  value       = aws_s3_bucket.tfstate.bucket
}

output "state_lock_method" {
  description = "How Terraform state is locked. S3 conditional writes, not DynamoDB."
  value       = "s3 backend use_lockfile"
}

output "manifest_table" {
  description = "Ingestion manifest. One row per source hour."
  value       = aws_dynamodb_table.manifest.name
}

output "curated_bucket" {
  description = "Curated Parquet, plus the 48-hour source gz regression fixture."
  value       = aws_s3_bucket.curated.bucket
}

output "athena_workgroup" {
  description = "Scan-limited Athena workgroup. Every query must run in this workgroup."
  value       = aws_athena_workgroup.main.name
}

output "athena_scan_limit_gib" {
  description = "Per-query scan limit, in GiB."
  value       = var.athena_scan_limit_bytes / 1024 / 1024 / 1024
}

output "athena_results_bucket" {
  description = "Where query results land. Results expire automatically."
  value       = aws_s3_bucket.athena_results.bucket
}

output "alerts_topic_arn" {
  description = "SNS topic both cost tripwires publish to."
  value       = aws_sns_topic.alerts.arn
}

output "next_steps" {
  description = "What has to happen by hand after this apply."
  value = join("\n", [
    "1. Click the confirmation link in the SNS email sent to ${var.contact_email}.",
    "2. Once the tripwire-test budget emails you, set create_tripwire_test_budget = false and apply.",
  ])
}
