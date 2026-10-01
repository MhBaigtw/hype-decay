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
