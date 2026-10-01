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
    condition     = var.region == "us-east-1"
    error_message = "CLAUDE.md pins this project to us-east-1."
  }
}

variable "deploy_role_trusted_principals" {
  description = <<-EOT
    Who may assume the deployment role. Deliberately the human SSO admin role
    and nothing else: the account root ARN is NOT listed, because trusting it
    would let any principal in the account assume this role. If the
    AdministratorAccess permission set is recreated, this ARN changes and must
    be updated here.
  EOT
  type        = list(string)
  default = [
    "arn:aws:iam::820697996849:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_AdministratorAccess_c0149b0ede28fc0a",
  ]
}

variable "backfill_instance_types" {
  description = <<-EOT
    The only EC2 instance types the deploy role may launch, enforced as an IAM
    condition on ec2:RunInstances -- which is what actually stops a larger
    instance, where a Terraform variable would only stop Terraform.

    Burstable types are deliberately absent. The parse is CPU-bound at 5.2 s per
    hour-file, and a t4g.small sustains only 20% of its 2 vCPUs once burst
    credits run out, which would stretch 25.3 CPU-hours of parsing to roughly
    63 h of stalling. Compute-optimized Graviton only.
  EOT
  type        = list(string)
  default     = ["c7g.medium", "c7g.large", "c7g.xlarge"]
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

variable "create_tripwire_test_budget" {
  description = <<-EOT
    Creates an extra 0.01 USD budget whose only purpose is to prove the alert
    path delivers email. A 10 USD budget alerting at 50% needs 5 USD of spend to
    fire, which this project should never reach, so the real guardrail would go
    unobserved. Destroy this budget once the email arrives. AWS Budgets is free
    for the first two budgets, and two is all we have.
  EOT
  type        = bool
  default     = true
}

variable "athena_scan_limit_bytes" {
  description = "Interactive per-query scan limit. 5 GiB, per TASKS.md Task 1."
  type        = number
  default     = 5368709120
}

variable "athena_dbt_scan_limit_bytes" {
  description = <<-EOT
    Per-query scan limit for the dbt workgroup, deliberately higher than the
    interactive cap because the baseline model legitimately reads the whole
    page_daily history. Set from the measured compacted table size; see NOTES.
  EOT
  type        = number
  default     = 68719476736 # 64 GiB
}
