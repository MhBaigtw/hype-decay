# ---------------------------------------------------------------------------
# Both Athena workgroups live here, for the same reason as the budgets: a scan
# limit the deployer can raise is not a limit. The deploy role is denied
# athena:UpdateWorkGroup and athena:DeleteWorkGroup on both of these (see
# iam.tf), so a query that trips a cap has to be fixed rather than accommodated.
#
# The results bucket itself stays in the main module: it is storage, not a
# guardrail, and it has a lifecycle rule the pipeline legitimately owns. These
# workgroups reference it by NAME rather than by resource, so the two modules
# stay independent -- the cost is that renaming the bucket means editing both.
# ---------------------------------------------------------------------------

locals {
  athena_results_bucket = "${var.project}-athena-results-${data.aws_caller_identity.current.account_id}"
}

# Interactive queries. 5 GiB, per TASKS.md Task 1, and demonstrated firing in
# Task 1: an unpartitioned COUNT(*) over a public dataset was cancelled at
# exactly 5.00 GiB with "Bytes scanned limit was exceeded".
resource "aws_athena_workgroup" "main" {
  name        = var.project
  description = "Scan-limited workgroup. Every query must filter on a partition column."
  state       = "ENABLED"

  configuration {
    bytes_scanned_cutoff_per_query     = var.athena_scan_limit_bytes
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${local.athena_results_bucket}/query-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# dbt models. A higher cap, stated rather than smuggled.
#
# The 5 GiB interactive cap exists to kill a careless SELECT *. The baseline
# model is not careless: it reads the whole page_daily history to compute a
# trailing 28-day median per page-day, which is a legitimate whole-table read.
# Forcing it under 5 GiB would mean chunking the model into hundreds of runs or
# shortening the window and lying about it. Two workgroups with two honest caps
# is the better answer, and each cap is still a hard stop.
resource "aws_athena_workgroup" "dbt" {
  name        = "${var.project}-dbt"
  description = "dbt models. Higher scan cap than interactive, still enforced."
  state       = "ENABLED"

  configuration {
    bytes_scanned_cutoff_per_query     = var.athena_dbt_scan_limit_bytes
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${local.athena_results_bucket}/dbt-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
