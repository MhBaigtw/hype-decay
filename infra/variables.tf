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

variable "backfill_instance_enabled" {
  description = <<-EOT
    Whether the backfill instance exists. Defaults to false so the committed
    state of this repo has no instance with an hourly cost in it. Turn it on
    for a measured run, then turn it off:
      terraform apply -var backfill_instance_enabled=true
      terraform apply -var backfill_instance_enabled=false
  EOT
  type        = bool
  default     = false
}

variable "backfill_instance_type" {
  description = <<-EOT
    Instance type for the backfill box. Compute-optimized Graviton, not
    burstable.

    Measured: the pyarrow parse costs 5.2 s per hour-file, so 17,520 files is
    25.3 CPU-hours. A t4g.small sustains only 20% of its 2 vCPUs without
    burst credits, so a CPU-bound parse would throttle to roughly 63 h and
    stall whenever credits ran out. c7g gives full-rate cores.

    Cost is NOT the deciding factor: on-demand price scales linearly with
    vCPU (c7g.medium $0.0363/h, large $0.0725, xlarge $0.1450), so the parse
    costs about $0.92 at any size and only the wall clock changes -- 25.3 h on
    1 core, 6.3 h on 4. Against roughly 5.0 h of download at mirror speed,
    4 cores balance the two stages, so xlarge is the choice. 8 GiB also leaves
    headroom for 4 concurrent pyarrow parses; the measurement run reports the
    real peak RSS per worker.

    Must appear in the bootstrap module backfill_instance_types list, which is
    the IAM condition that actually enforces the ceiling.
  EOT
  type        = string
  default     = "c7g.xlarge"
}

variable "athena_dbt_scan_limit_bytes" {
  description = <<-EOT
    Per-query scan limit for the dbt workgroup, which is deliberately higher
    than the 5 GiB interactive cap.

    The interactive cap exists to kill a careless SELECT *. The dbt baseline
    model is not careless: it legitimately reads the whole page_daily history
    to compute a trailing 28-day median per page-day. Forcing that under 5 GiB
    would mean either chunking the model into 200 runs or lying about the
    window. Two workgroups with two honest caps is the better answer, and each
    cap is still a hard stop.

    Set from the measured compacted table size; see NOTES.
  EOT
  type        = number
  default     = 68719476736 # 64 GiB
}

variable "backfill_time_box_minutes" {
  description = <<-EOT
    Hard time box. user_data schedules a shutdown this many minutes after boot
    and the instance terminates rather than stops, so a forgotten box bills for
    this long and no longer. CLAUDE.md requires the box to be stated before
    launch.
  EOT
  type        = number
  default     = 180
}

variable "athena_results_retention_days" {
  description = "Athena query results are disposable; expire them so they stop costing storage."
  type        = number
  default     = 7
}
