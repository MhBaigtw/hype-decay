variable "project" {
  description = "Name prefix for every resource in this project."
  type        = string
  default     = "hype-decay"
}

variable "region" {
  description = "The only region this project uses."
  type        = string
  default     = "us-east-1"

  validation {
    # CLAUDE.md: region is us-east-1, everything, no exceptions. CloudWatch
    # billing metrics only exist here anyway.
    condition     = var.region == "us-east-1"
    error_message = "CLAUDE.md pins this project to us-east-1."
  }
}

variable "contact_email" {
  description = "Where budget and alarm notifications go. Same address as the Wikimedia User-Agent contact."
  type        = string
  default     = "MhBaig971@gmail.com"
}

variable "budget_limit_usd" {
  description = "Monthly cost budget. TASKS.md Task 1 specifies 10 USD."
  type        = number
  default     = 10
}

variable "budget_alert_percentages" {
  description = "Percent-of-budget thresholds that trigger an email."
  type        = list(number)
  default     = [50, 80, 100]
}

variable "billing_alarm_threshold_usd" {
  description = "Second, independent tripwire: a CloudWatch alarm on estimated charges."
  type        = number
  default     = 5
}

variable "athena_scan_limit_bytes" {
  description = "Per-query data scan limit. 5 GiB, per TASKS.md Task 1."
  type        = number
  default     = 5368709120
}

variable "create_tripwire_test_budget" {
  description = <<-EOT
    Creates an extra 0.01 USD budget whose only purpose is to prove the alert
    path actually delivers email. A 10 USD budget alerting at 50% would need
    5 USD of spend to fire, which this project should never reach, so the real
    guardrail would go unobserved. Destroy this budget once the email arrives.
    AWS Budgets bills 0.02 USD/day beyond the first two budgets; two is all we
    have, so this costs nothing.
  EOT
  type        = bool
  default     = true
}

variable "deploy_role_trusted_principals" {
  description = <<-EOT
    Who may assume the Terraform deployment role. Deliberately the human SSO
    admin role and nothing else: the account root ARN is NOT listed, because
    trusting it would let any principal in the account assume this role.
    If the AdministratorAccess permission set is ever recreated, this ARN
    changes and must be updated.
  EOT
  type        = list(string)
  default = [
    "arn:aws:iam::820697996849:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_AdministratorAccess_c0149b0ede28fc0a",
  ]
}

variable "backfill_instance_types" {
  description = "The only EC2 instance types the deploy role may launch, per the CLAUDE.md backfill exception."
  type        = list(string)
  default     = ["t4g.small", "t3.small"]
}

variable "athena_results_retention_days" {
  description = "Athena query results are disposable; expire them so they stop costing storage."
  type        = number
  default     = 7
}
