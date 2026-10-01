output "deploy_role_arn" {
  description = "Least-privilege role every other module is applied as. Not for interactive use."
  value       = aws_iam_role.deploy.arn
}

output "alerts_topic_arn" {
  description = "SNS topic both cost tripwires publish to."
  value       = aws_sns_topic.alerts.arn
}

output "athena_workgroup" {
  description = "Interactive workgroup. Every query must run in a workgroup with a cap."
  value       = aws_athena_workgroup.main.name
}

output "athena_dbt_workgroup" {
  description = "Workgroup for dbt models, with a higher but still enforced cap."
  value       = aws_athena_workgroup.dbt.name
}

output "athena_scan_limits_gib" {
  description = "Both caps, in GiB: interactive and dbt."
  value = {
    interactive = var.athena_scan_limit_bytes / 1024 / 1024 / 1024
    dbt         = var.athena_dbt_scan_limit_bytes / 1024 / 1024 / 1024
  }
}

output "how_to_use" {
  description = "Which profile applies which module, and why."
  value = join("\n", [
    "bootstrap (this module): AWS_PROFILE=hype-decay        (human admin)",
    "everything else:         AWS_PROFILE=hype-decay-deploy (assumes the role below)",
    "the deploy role denies modifying itself and the guardrails here, so it",
    "cannot apply this module even by accident",
  ])
}

output "outstanding_manual_steps" {
  description = "What no apply can do for you."
  value = join("\n", [
    "1. Confirm the SNS email subscription. Until then the topic has no",
    "   subscribers and the billing alarm publishes into nothing.",
    "2. Once the tripwire-test budget emails you, set",
    "   create_tripwire_test_budget = false and apply.",
  ])
}
