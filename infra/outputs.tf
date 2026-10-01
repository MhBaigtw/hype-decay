output "state_bucket" {
  description = "Terraform state bucket. This module keeps its state under guardrails/."
  value       = aws_s3_bucket.tfstate.bucket
}

output "state_lock_method" {
  description = "How Terraform state is locked. S3 conditional writes, not DynamoDB."
  value       = "s3 backend use_lockfile"
}

output "manifest_table" {
  description = "Ingestion manifest: one row per source hour, plus a day# row per compacted day."
  value       = aws_dynamodb_table.manifest.name
}

output "curated_bucket" {
  description = "Curated Parquet, plus the 48-hour source gz regression fixture."
  value       = aws_s3_bucket.curated.bucket
}

output "athena_results_bucket" {
  description = "Where query results land. Results expire automatically."
  value       = aws_s3_bucket.athena_results.bucket
}

output "owned_by_bootstrap" {
  description = "What this module deliberately does NOT manage, and where it lives."
  value = join("\n", [
    "deploy role, budgets, billing alarm, SNS alerts topic and both Athena",
    "workgroups live in infra/bootstrap, applied with AWS_PROFILE=hype-decay.",
    "This module is applied as the deploy role, which is denied delete and",
    "modify on every one of them.",
  ])
}
