# ---------------------------------------------------------------------------
# Athena workgroup with a hard per-query scan limit.
#
# Athena bills per byte scanned. One careless SELECT * over two years of data
# is the fastest way to spend this project budget in a single query, so the
# workgroup refuses any query that would scan more than the limit. The limit is
# enforced at the workgroup level (enforce_workgroup_configuration) so a client
# cannot override it by sending its own settings.
#
# CLAUDE.md: if a query trips this, fix the query. Do not raise the limit.
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.project}-athena-results-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "expire-query-results"
    status = "Enabled"

    filter {}

    # Query results are reproducible by re-running the query, so keeping them
    # is pure cost.
    expiration {
      days = var.athena_results_retention_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_athena_workgroup" "main" {
  name        = var.project
  description = "Scan-limited workgroup. Every query must filter on a partition column."
  state       = "ENABLED"

  configuration {
    # The guardrail itself.
    bytes_scanned_cutoff_per_query = var.athena_scan_limit_bytes

    # Clients cannot substitute their own result location or scan limit.
    enforce_workgroup_configuration = true

    # Needed to see bytes-scanned per query, which Task 4 has to report.
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/query-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# ---------------------------------------------------------------------------
# A second workgroup, for dbt.
#
# The 5 GiB interactive cap would kill the full-history dbt models: int_baselines
# reads every page-day to compute a trailing 28-day median, which is a legitimate
# whole-table read, not a careless one. Raising the interactive cap to fit it
# would remove the guardrail that catches genuine mistakes, so instead there are
# two workgroups with two stated caps, and dbt runs in this one.
#
# CLAUDE.md says: if a query trips the limit, fix the query, do not raise the
# limit. That still holds. This is not a raised limit; it is a different limit
# for a different, declared workload -- and it is still a hard stop.
# ---------------------------------------------------------------------------

resource "aws_athena_workgroup" "dbt" {
  name        = "${var.project}-dbt"
  description = "dbt models. Higher scan cap than interactive, still enforced."
  state       = "ENABLED"

  configuration {
    bytes_scanned_cutoff_per_query     = var.athena_dbt_scan_limit_bytes
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/dbt-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
