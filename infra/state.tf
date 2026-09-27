# ---------------------------------------------------------------------------
# Terraform remote state: an S3 bucket for the state file, a DynamoDB table for
# the lock.
#
# Why both: S3 holds the state, and the lock table stops two applies running at
# once and corrupting it. Terraform can now also lock with a file in S3
# (use_lockfile), which would make this table unnecessary -- but TASKS.md asks
# for the DynamoDB table, and on-demand billing makes it effectively free at
# this scale. Noted in NOTES.md as a decision to revisit.
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "tfstate" {
  # Bucket names are globally unique, so the account id is the suffix.
  bucket = "${var.project}-tfstate-${data.aws_caller_identity.current.account_id}"

  # State is the one thing here that is genuinely painful to lose: without it,
  # Terraform no longer knows what it created and every resource has to be
  # imported by hand. To tear this project down, comment this block out first.
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id

  # Versioning is what turns a bad apply into a recoverable mistake.
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "tfstate" {
  bucket                  = aws_s3_bucket.tfstate.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# State files can contain resource attributes that are effectively secrets, so
# refuse any request that is not over TLS.
data "aws_iam_policy_document" "tfstate_tls_only" {
  statement {
    sid    = "DenyInsecureTransport"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = ["s3:*"]

    resources = [
      aws_s3_bucket.tfstate.arn,
      "${aws_s3_bucket.tfstate.arn}/*",
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  policy = data.aws_iam_policy_document.tfstate_tls_only.json
}

resource "aws_dynamodb_table" "tflock" {
  name         = "${var.project}-tflock"
  billing_mode = "PAY_PER_REQUEST" # CLAUDE.md: DynamoDB on-demand only
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  lifecycle {
    prevent_destroy = true
  }
}
