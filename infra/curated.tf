# ---------------------------------------------------------------------------
# The curated zone and the ingestion manifest. Task 2 onwards.
#
# There is no raw zone (SPEC storage model): the ingester converts to Parquet in
# flight, and the only source bytes kept are 48 hours of gz held as a
# format-regression fixture. Everything else is re-fetchable from Wikimedia,
# which recon proved to be a complete archive of every hour in the window.
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "curated" {
  bucket = "${var.project}-curated-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_public_access_block" "curated" {
  bucket                  = aws_s3_bucket.curated.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "curated" {
  bucket = aws_s3_bucket.curated.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Deliberately NOT versioned. Curated Parquet is derived data: it is
# reproducible from the source URL plus the manifest, so paying to keep old
# versions of it would be paying twice for the same reproducibility.

resource "aws_s3_bucket_lifecycle_configuration" "curated" {
  bucket = aws_s3_bucket.curated.id

  # The regression fixture, and only the fixture, expires.
  rule {
    id     = "expire-raw-fixture"
    status = "Enabled"

    filter {
      prefix = "fixtures/raw_48h/"
    }

    # SPEC says 48 hours. S3 lifecycle granularity is whole days and the
    # expiry sweep runs about once a day, so real retention is 2 to 3 days.
    # That is the closest S3 gets to 48 hours; anything tighter needs a
    # scheduled delete, which is not worth a Lambda at this size.
    expiration {
      days = 2
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

# ---------------------------------------------------------------------------
# Ingestion manifest.
#
# With no raw zone, this table is the record of what was fetched and what it
# contained. It is what makes an hour re-fetchable and byte-verifiable, and in
# Task 3 it is also the progress view for a backfill of 17,520 hours.
# ---------------------------------------------------------------------------

resource "aws_dynamodb_table" "manifest" {
  name         = "${var.project}-manifest"
  billing_mode = "PAY_PER_REQUEST" # CLAUDE.md: DynamoDB on-demand only
  hash_key     = "source_hour"

  # source_hour is the hour in the SOURCE FILENAME, e.g. 2026-09-10T18, which
  # is the END of the capture window. The row also stores hour_start, which is
  # that minus one hour. Keying on the filename hour keeps the manifest keyed
  # by the thing actually fetched, so a re-fetch is unambiguous.
  attribute {
    name = "source_hour"
    type = "S"
  }

  attribute {
    name = "status"
    type = "S"
  }

  # Task 3 needs to answer "what is still pending or failed" across 17,520
  # rows without scanning the table. KEYS_ONLY keeps the index small.
  global_secondary_index {
    name            = "status-index"
    hash_key        = "status"
    range_key       = "source_hour"
    projection_type = "KEYS_ONLY"
  }
}
