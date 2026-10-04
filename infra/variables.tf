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
    real peak RSS per parse worker AND for compacting one day, which overlaps
    the next day's parsing, and holds the sum against this instance's RAM.

    Must appear in the bootstrap module backfill_instance_types list, which is
    the IAM condition that actually enforces the ceiling.
  EOT
  type        = string
  default     = "c7g.xlarge"
}

variable "backfill_ami_id" {
  description = <<-EOT
    The backfill AMI, pinned. al2023-ami-2023.12.20260930.0-kernel-6.18-arm64,
    created 2026-09-29, deprecates 2026-12-28 (still launchable by id after).
    Changing this REPLACES the instance, so never change it while one is running.
  EOT
  type        = string
  default     = "ami-065b1b834d2a83a7a"
}

variable "backfill_kernel" {
  description = <<-EOT
    The AL2023 kernel line backfill_ami_id must carry; checked at plan time.

    6.18, chosen rather than inherited. Amazon publishes 6.1, 6.12 and 6.18
    arm64 images with identical timestamps, and as of 2026-09-30 its own SSM
    parameter al2023-ami-kernel-default-arm64 resolves to the 6.18 image. 6.1
    was pinned earlier only to stop most_recent flipping between lines; pinning
    the AMI id now does that job, so the kernel can be chosen on its merits.

    The workload is one HTTP GET stream, pyarrow on four cores and S3 PUTs. It
    needs nothing from any particular kernel, so the deciding question is which
    line carries the least surprise, and the answer is the one Amazon ships as
    the default and therefore tests hardest. The box lives three hours, so
    support lifetime does not enter into it.
  EOT
  type        = string
  default     = "6.18"
}

variable "backfill_python" {
  description = "Interpreter version installed from the AL2023 repos. The system python3 is 3.9."
  type        = string
  default     = "3.12"
}

variable "backfill_pip_pins" {
  description = <<-EOT
    Exact package versions for the instance: the ones the parser regression test
    and the Task 2 measurements ran on. Unpinned, the box would measure code the
    laptop never ran.
  EOT
  type        = list(string)
  default     = ["pyarrow==25.0.1", "boto3==1.43.103"]
}

variable "backfill_code_commit" {
  description = <<-EOT
    The git commit uploaded to s3://<curated>/code/, whose COMMIT file must
    match or user_data refuses to start. Recorded in NOTES.md with the run.
    Changing it changes user_data and so REPLACES the instance -- update it only
    between launches.
  EOT
  type        = string
  default     = "7390248276c67fc486d86915e0454fa5466a6508" # uploaded 2026-10-03

  validation {
    condition     = var.backfill_code_commit == "" || can(regex("^[0-9a-f]{40}$", var.backfill_code_commit))
    error_message = "backfill_code_commit must be a full 40-character commit hash."
  }
}

variable "backfill_time_box_minutes" {
  description = <<-EOT
    Hard time box. user_data schedules a shutdown this many minutes after boot
    and the instance terminates rather than stops, so a forgotten box bills for
    this long and no longer. CLAUDE.md requires the box to be stated before
    launch.

    1080 minutes (18 h) for the full backfill, sized from the 3-day trial on
    2026-10-04: 66.6 s a day measured, of which about 9 s was the trial-only
    verification copy, so about 58 s a day in production. 730 days x 58 s is
    11.8 h; 1.5x margin is 17.6 h, plus boot, so 18 h. At $0.145/h that caps a
    forgotten box at $2.61. If the timer does fire early the runner resumes
    from the manifest on the next launch, so an undersized box costs a relaunch,
    not data. A trial passes a smaller box with -var.
  EOT
  type        = number
  default     = 1080
}

variable "athena_results_retention_days" {
  description = "Athena query results are disposable; expire them so they stop costing storage."
  type        = number
  default     = 7
}
