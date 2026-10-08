# ---------------------------------------------------------------------------
# Terraform remote state: an S3 bucket, locked by a lock file in that same
# bucket.
#
# There are two ways to lock Terraform state, and this project has used both.
#
#   1. DynamoDB table. A table holds a LockID item for the duration of an
#      apply, wired up with the backend dynamodb_table parameter. This is what
#      Task 1 built.
#   2. S3 conditional writes (use_lockfile = true). Terraform puts a .tflock
#      object next to the state file and relies on S3 refusing a conditional
#      write when the object already exists.
#
# Switched to (2) and destroyed the table, because Terraform 1.16 deprecates
# dynamodb_table and warns on every init. Same mutual exclusion, one fewer
# resource to create, pay for and reason about. The trade-off: locking now
# depends on S3 semantics alone, so a bucket-level permission mistake could
# remove the lock without removing access to the state. The old table
# definition is in git history if it is ever needed again.
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "tfstate" {
  # Bucket names are globally unique, so the account id is the suffix.
  bucket = "${var.project}-tfstate-${data.aws_caller_identity.current.account_id}"
  tags   = { Task = "task-1-guardrails" }

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
