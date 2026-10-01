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
    page_daily history.

    25 GiB, from measurement. 2026-09-10 compacted at floor 10 is 24,256,661
    bytes, all five columns (page_title alone is 20.5 MB of it). Over 730 days
    that is 16.5 GiB for a full read of every column; 25 GiB is 1.5x of that.
    The earlier 64 GiB was the FLOOR 0 projection (92.4 MB a day), which the
    floor made 3.8x too generous.

    What the cap assumes: no single dbt query reads page_daily more than once.
    The baseline needs the candidate pages AND their history, which written as
    one query is two full scans, about 33 GiB, and would trip this. Candidate
    pages are therefore their own model, so each query scans the table once.
    If a query trips the cap, fix the query (CLAUDE.md); re-measure this only if
    the backfilled table turns out larger than 16.5 GiB.
  EOT
  type        = number
  default     = 26843545600 # 25 GiB
}
