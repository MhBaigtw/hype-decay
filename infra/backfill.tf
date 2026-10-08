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

# The AMI is PINNED by id (var.backfill_ami_id), not resolved at plan time.
#
# A most_recent lookup makes the AMI id a function of the day you plan. Amazon
# published a new AL2023 set on 2026-09-29, so a plan run the day after launch
# would have shown a new id, and a changed ami on aws_instance is a forced
# REPLACEMENT: one careless apply mid-transfer and the box is destroyed and
# relaunched. Pinned, the instance changes only when someone edits the variable,
# and the plan says so.
#
# This data source does not choose the image. It looks up the pinned id and
# fails the plan if that id is not what the variable claims: Amazon-owned,
# arm64, and the kernel line chosen below.
#
# Correction to an earlier comment here, which said there is no SSM public
# parameter for AL2023. There is:
#   /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64
# The lookup that "failed" was most likely run from Git Bash, which rewrites a
# leading /aws/... into a Windows path before the CLI sees it -- reproduced on
# 2026-09-30: ParameterNotFound from Git Bash, the AMI id with MSYS_NO_PATHCONV=1.
# The parameter is useful for picking the next id by hand; it is still not
# used here, because any latest-pointer reintroduces the replacement risk above.
data "aws_ami" "backfill" {
  owners             = ["amazon"]
  include_deprecated = true # pinned ids deprecate after ~90 days; still launchable

  filter {
    name   = "image-id"
    values = [var.backfill_ami_id]
  }

  lifecycle {
    postcondition {
      condition     = self.architecture == "arm64"
      error_message = "backfill_ami_id must be an arm64 image: the instance type is Graviton."
    }
    postcondition {
      condition     = can(regex("^al2023-ami-2023\\.[0-9.]+-kernel-${replace(var.backfill_kernel, ".", "\\.")}-arm64$", self.name))
      error_message = "backfill_ami_id is not an AL2023 kernel-${var.backfill_kernel} arm64 image."
    }
  }
}

# --- network ---------------------------------------------------------------

resource "aws_security_group" "backfill" {
  name        = "${var.project}-backfill"
  description = "Backfill instance. Egress only; nothing may connect inbound."
  vpc_id      = data.aws_vpc.default.id
  tags        = { Task = "task-3-backfill" }

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
  tags               = { Task = "task-3-backfill" }
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

  # run_backfill.sh arms the stall alarm when it starts and disarms it after a
  # clean finish. This alarm only: the billing alarm is out of reach.
  statement {
    sid       = "ArmOwnStallAlarm"
    effect    = "Allow"
    actions   = ["cloudwatch:EnableAlarmActions", "cloudwatch:DisableAlarmActions"]
    resources = ["arn:aws:cloudwatch:${var.region}:${data.aws_caller_identity.current.account_id}:alarm:${var.project}-backfill-stall"]
  }

  # The wrapper waits for the alarm to read OK before arming it, so arming never
  # lands mid-recovery and turns the start of every run into a recovery email.
  # Read-only, and granted on "*" rather than an alarm ARN on purpose: if a
  # resource-scoped grant were wrong, the read would fail and the wrapper would
  # wait out its fallback instead of arming promptly -- a silent degradation.
  statement {
    sid       = "ReadAlarmState"
    effect    = "Allow"
    actions   = ["cloudwatch:DescribeAlarms"]
    resources = ["*"]
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
  tags = { Task = "task-3-backfill" }
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

    # The system python3 on AL2023 is 3.9, and an unpinned pip install there
    # resolves to whatever old pyarrow still supports 3.9 -- not the version
    # the parser was verified on. So: a current interpreter, pinned packages.
    dnf -y install python${var.backfill_python} python${var.backfill_python}-pip
    python${var.backfill_python} -m pip install --quiet ${join(" ", var.backfill_pip_pins)}

    mkdir -p /opt/${var.project}
    aws s3 cp s3://${aws_s3_bucket.curated.bucket}/code/ /opt/${var.project}/ \
      --recursive --region ${var.region}
    chmod -R a+rx /opt/${var.project}

    # Refuse to run code other than the commit this launch was planned with.
    # Without this check, whatever was last uploaded to code/ is what runs, and
    # nothing records which commit produced the backfill.
    got="$(cat /opt/${var.project}/COMMIT)"
    if [ "$got" != "${var.backfill_code_commit}" ]; then
      echo "code/ holds $got, expected ${var.backfill_code_commit}" > /opt/${var.project}/WRONG_CODE
      exit 1
    fi

    # Marker the operator can poll for, so "is it ready" has a real answer.
    { date -u +%FT%TZ; echo "commit $got"; uname -r; python${var.backfill_python} -c \
      'import pyarrow, boto3; print("pyarrow", pyarrow.__version__, "boto3", boto3.__version__)'; } \
      > /opt/${var.project}/READY
  BASH
}

resource "aws_instance" "backfill" {
  count = local.backfill_count

  ami           = data.aws_ami.backfill.id
  instance_type = var.backfill_instance_type
  subnet_id     = data.aws_subnets.default.ids[0]

  vpc_security_group_ids = [aws_security_group.backfill.id]
  iam_instance_profile   = aws_iam_instance_profile.backfill.name

  # The important line: shutdown means gone, not parked with a billing volume.
  instance_initiated_shutdown_behavior = "terminate"

  user_data                   = local.backfill_user_data
  user_data_replace_on_change = true

  lifecycle {
    precondition {
      condition     = var.backfill_code_commit != ""
      error_message = "Set backfill_code_commit to the commit uploaded to code/ before launching."
    }
  }

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
    Task       = "task-3-backfill"
    TimeBoxMin = tostring(var.backfill_time_box_minutes)
    CodeCommit = var.backfill_code_commit
  }
}

# --- stall alarm -------------------------------------------------------------
#
# Run 1 of the backfill died mid-window and nothing said so for ten hours. This
# alarm fires if no day is compacted for 20 minutes: four 5-minute periods in a
# row where DaysCompacted sums below 1. Missing data counts as breaching, since
# a dead box publishes nothing at all, which is the case that matters most.
#
# It exists only alongside the instance (same count) and is CREATED DISARMED.
# Run 2 created it armed: with no data yet and missing data breaching, it went
# to ALARM within seconds of creation and emailed a false stall before the first
# day had even started. Now run_backfill.sh arms it only once the first day has
# compacted AND the alarm reads OK, and disarms it after a clean finish. So it
# is silent at launch and between runs, but a box that dies mid-run leaves it
# armed, and both the stall and the recovery (ok_actions) are emailed.
# ignore_changes stops Terraform fighting the wrapper over the flag. It
# publishes to the bootstrap-owned alerts topic, referenced by name because the
# two modules keep separate state.
#
# Cost: one standard alarm, $0.10/month prorated, for as long as it exists.

resource "aws_cloudwatch_metric_alarm" "backfill_stall" {
  count = local.backfill_count

  alarm_name        = "${var.project}-backfill-stall"
  alarm_description = "No backfill day compacted for 20 minutes. Check the manifest and s3://${aws_s3_bucket.curated.bucket}/logs/backfill/."

  namespace   = "${var.project}/backfill"
  metric_name = "DaysCompacted"
  statistic   = "Sum"
  period      = 300

  evaluation_periods  = 4
  datapoints_to_alarm = 4
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"

  actions_enabled = false
  alarm_actions   = ["arn:aws:sns:${var.region}:${data.aws_caller_identity.current.account_id}:${var.project}-alerts"]
  ok_actions      = ["arn:aws:sns:${var.region}:${data.aws_caller_identity.current.account_id}:${var.project}-alerts"]

  tags = { Task = "task-3-backfill" }

  lifecycle {
    ignore_changes = [actions_enabled]
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
