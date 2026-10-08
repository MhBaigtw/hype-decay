# ---------------------------------------------------------------------------
# Athena query results bucket.
#
# The WORKGROUPS moved to infra/bootstrap. A scan limit the deployer can raise is
# not a limit, so both workgroups are admin-owned and the deploy role is denied
# athena:UpdateWorkGroup and athena:DeleteWorkGroup on them.
#
# This bucket stays here: it is storage rather than a guardrail, and its
# lifecycle rule is something the pipeline legitimately owns.
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.project}-athena-results-${data.aws_caller_identity.current.account_id}"
  tags   = { Task = "task-1-guardrails" }
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

  # Query results are reproducible by re-running the query, so keeping them is
  # pure cost: they expire. dbt's TABLES must not, and they live in this bucket
  # too -- the dbt workgroup enforces its output location, so dbt-athena ignores
  # s3_data_dir and writes table data under dbt-results/tables/. A single
  # bucket-wide expiry (the rule this replaces) would have deleted every model's
  # data 7 days after each build, and the tables would have read empty.
  #
  # S3 lifecycle filters match prefixes, so "dbt-results/ except tables/" is
  # expressed by what Athena names things: every query result is
  # <query-id>.csv[.metadata], and a query id is a UUID, so those keys begin
  # with a hex digit. dbt-results/tables/ begins with "t", which no rule below
  # matches. Checked against the bucket on 2026-10-08: every top-level object
  # under dbt-results/ started with 0-9 or a-f, and every table object with "t".
  rule {
    id     = "expire-query-results"
    status = "Enabled"

    filter {
      prefix = "query-results/"
    }

    expiration {
      days = var.athena_results_retention_days
    }
  }

  dynamic "rule" {
    for_each = toset(split("", "0123456789abcdef"))

    content {
      id     = "expire-dbt-query-results-${rule.value}"
      status = "Enabled"

      filter {
        prefix = "dbt-results/${rule.value}"
      }

      expiration {
        days = var.athena_results_retention_days
      }
    }
  }

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}
