output "state_bucket" {
  description = "Terraform state bucket. Goes in the backend block in versions.tf."
  value       = aws_s3_bucket.tfstate.bucket
}

output "lock_table" {
  description = "DynamoDB state lock table."
  value       = aws_dynamodb_table.tflock.name
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

output "deploy_role_arn" {
  description = "Least-privilege role the pipeline assumes. Not for interactive use."
  value       = aws_iam_role.deploy.arn
}

output "next_steps" {
  description = "What has to happen by hand after this apply."
  value = join("\n", [
    "1. Click the confirmation link in the SNS email sent to ${var.contact_email}.",
    "2. Uncomment the backend block in versions.tf, then run: terraform init -migrate-state",
    "3. Demonstrate the Athena scan limit rejecting an unpartitioned query.",
    "4. Once the tripwire-test budget has emailed you, set create_tripwire_test_budget = false and apply.",
  ])
}
