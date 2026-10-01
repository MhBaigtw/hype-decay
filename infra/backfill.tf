# ---------------------------------------------------------------------------
# The backfill instance. CLAUDE.md permits exactly one small instance, for the
# backfill only, and it is the single exception to the no-idle-cost rule.
#
# Three things keep it from becoming a permanent cost:
#
#   1. var.backfill_instance_enabled defaults to FALSE, so the committed state
#      of this repo has no instance in it. Turning it on is an explicit act.
#   2. instance_initiated_shutdown_behavior = "terminate" plus a shutdown timer
#      in user_data: the box kills itself after the time box even if everyone
#      forgets about it. Stopping would keep the EBS volume billing; terminating
#      does not.
#   3. The root volume is delete_on_termination.
#
# The IAM role, instance profile and security group are created unconditionally.
# None of them costs anything idle, and having them ready means the instance can
# be turned on and off without a dependency dance.
# ---------------------------------------------------------------------------

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# Amazon Linux 2023 for arm64, resolved at plan time rather than pinned to an
# AMI id that goes stale.
#
# This was an SSM public-parameter lookup first, and it failed: there is no
# /aws/service/ami-al2023-latest namespace, and public parameter paths cannot be
# enumerated to find the right one because GetParametersByPath rejects /aws/
# outright. describe-images needs nothing beyond the ec2:Describe* the deploy
# role already has, and the result can be read back and checked by hand.
#
# The filter pins the 6.1 kernel line deliberately. Amazon publishes 6.1 and
# 6.12 arm64 AMIs with identical creation dates, so an unpinned most_recent
# would flip kernel versions between plans for no stated reason.
data "aws_ami" "al2023_arm64" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2*-kernel-6.1-arm64"]
  }

  filter {
    name   = "state"
    values = ["available"]
  }
}

# --- network ---------------------------------------------------------------

resource "aws_security_group" "backfill" {
  name        = "${var.project}-backfill"
  description = "Backfill instance. Egress only; nothing may connect inbound."
  vpc_id      = data.aws_vpc.default.id

  # No ingress rules at all. Access is through SSM Session Manager, which works
  # outbound-only, so there is no SSH port and no key pair to leak.
  egress {
    description = "outbound to the mirror, S3, DynamoDB and SSM"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- instance identity -----------------------------------------------------

data "aws_iam_policy_document" "backfill_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "backfill" {
  name               = "${var.project}-backfill"
  description        = "Identity of the backfill instance. Narrower than the deploy role."
  assume_role_policy = data.aws_iam_policy_document.backfill_assume.json
}

data "aws_iam_policy_document" "backfill" {
  # The instance writes curated Parquet and the fixture, and reads its own code.
  statement {
    sid    = "CuratedBucketOnly"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:AbortMultipartUpload",
    ]

    resources = [
      aws_s3_bucket.curated.arn,
      "${aws_s3_bucket.curated.arn}/*",
    ]
  }

  # The manifest, and nothing else in DynamoDB.
  statement {
    sid    = "ManifestOnly"
    effect = "Allow"

    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:Query",
    ]

    resources = [
      aws_dynamodb_table.manifest.arn,
      "${aws_dynamodb_table.manifest.arn}/index/*",
    ]
  }

  # Progress visible without SSH, per TASKS Task 3.
  statement {
    sid       = "PublishProgressMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["${var.project}/backfill"]
    }
  }

  # Deny everything expensive outright, exactly as the deploy role does. An
  # instance with credentials is a place where a mistake becomes a bill.
  statement {
    sid    = "DenyForbiddenServices"
    effect = "Deny"

    actions = [
      "airflow:*", "redshift:*", "redshift-serverless:*", "rds:*", "es:*",
      "opensearch:*", "aoss:*", "elasticache:*", "neptune:*", "neptune-db:*",
      "sagemaker:*", "kinesis:*", "elasticmapreduce:RunJobFlow",
      "ec2:RunInstances", "ec2:CreateNatGateway", "ec2:AllocateAddress",
    ]

    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "backfill" {
  name   = "${var.project}-backfill"
  role   = aws_iam_role.backfill.id
  policy = data.aws_iam_policy_document.backfill.json
}

# Session Manager, so there is no inbound port and no SSH key anywhere.
resource "aws_iam_role_policy_attachment" "backfill_ssm" {
  role       = aws_iam_role.backfill.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "backfill" {
  name = "${var.project}-backfill"
  role = aws_iam_role.backfill.name
}

# --- the instance itself ---------------------------------------------------

locals {
  backfill_count = var.backfill_instance_enabled ? 1 : 0

  backfill_user_data = <<-BASH
    #!/bin/bash
    set -euxo pipefail

    # Dead man switch. Runs at boot, before anything else can go wrong, and the
    # instance is set to TERMINATE on shutdown rather than stop, so the volume
    # goes too. ${var.backfill_time_box_minutes} minutes is the time box.
    shutdown -h +${var.backfill_time_box_minutes}

    dnf -y install python3-pip
    python3 -m pip install --quiet boto3 pyarrow

    mkdir -p /opt/${var.project}
    aws s3 cp s3://${aws_s3_bucket.curated.bucket}/code/ /opt/${var.project}/ \
      --recursive --region ${var.region}
    chmod -R a+rx /opt/${var.project}

    # Marker the operator can poll for, so "is it ready" has a real answer.
    date -u +%FT%TZ > /opt/${var.project}/READY
  BASH
}

resource "aws_instance" "backfill" {
  count = local.backfill_count

  ami           = data.aws_ami.al2023_arm64.id
  instance_type = var.backfill_instance_type
  subnet_id     = data.aws_subnets.default.ids[0]

  vpc_security_group_ids = [aws_security_group.backfill.id]
  iam_instance_profile   = aws_iam_instance_profile.backfill.name

  # The important line: shutdown means gone, not parked with a billing volume.
  instance_initiated_shutdown_behavior = "terminate"

  user_data                   = local.backfill_user_data
  user_data_replace_on_change = true

  root_block_device {
    volume_size           = 20
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true
  }

  metadata_options {
    http_tokens = "required" # IMDSv2 only
  }

  tags = {
    Name       = "${var.project}-backfill"
    TimeBoxMin = tostring(var.backfill_time_box_minutes)
  }
}

output "backfill_instance_id" {
  description = "Instance id while the backfill box exists, empty when it does not."
  value       = try(aws_instance.backfill[0].id, "")
}

output "backfill_shutdown_check" {
  description = "How to confirm the time box is armed, and how to kill the box early."
  value = join("\n", [
    "aws ssm start-session --target <id> --profile hype-decay-deploy",
    "  then: shutdown --show   (expect a scheduled halt)",
    "terraform apply -var backfill_instance_enabled=false   (terminates it)",
    "aws ec2 describe-instances --instance-ids <id> --query Reservations[].Instances[].State.Name",
  ])
}
