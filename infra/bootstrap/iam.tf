# ---------------------------------------------------------------------------
# The deployment role. This file is the whole reason the bootstrap module exists.
#
# The human SSO identity is an administrator, but the pipeline is not. This role
# is what Terraform and the pipeline assume, and it is scoped three ways:
#
#   1. an Allow list covering only the services in the CLAUDE.md allow list
#   2. an explicit Deny on every forbidden service, which beats any Allow
#   3. a region Deny, so nothing can be created outside us-east-1 by accident
#
# The Deny list matters more than the Allow list. Allow lists drift wider over
# time as someone adds a permission to unblock themselves; a Deny cannot be
# overridden by a later Allow, so the expensive services stay unreachable even
# if the Allow list grows careless.
#
# It also denies modifying ITSELF. Without that, a role allowed to write IAM
# policies named hype-decay-* could rewrite its own and escape everything above.
# The cost of that safety is real: changes to this file must be applied with the
# admin profile, because the deploy role cannot apply them. That is the point.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "deploy_assume" {
  statement {
    sid     = "HumanAdminMayAssume"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = var.deploy_role_trusted_principals
    }
  }
}

resource "aws_iam_role" "deploy" {
  name                 = "${var.project}-deploy"
  description          = "Least-privilege deployment role for ${var.project}. Assumed by the SSO admin, never by root."
  assume_role_policy   = data.aws_iam_policy_document.deploy_assume.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "deploy" {
  # --- 1. the allow list ---------------------------------------------------
  statement {
    sid    = "AllowedServices"
    effect = "Allow"

    # CLAUDE.md allow list. ECS Fargate and Kinesis Firehose are deliberately
    # absent: they become allowed at v3, not now.
    actions = [
      "glue:*",
      "athena:*",
      "lambda:*",
      "states:*",
      "scheduler:*",
      "events:*",
      "dynamodb:*",
      "sns:*",
      "cloudwatch:*",
      "logs:*",
      "apigateway:*",
      "budgets:*",
      "tag:Get*",
      "ce:Get*",
    ]

    resources = ["*"]
  }

  # --- 1b. S3, scoped to this project -------------------------------------
  statement {
    sid    = "ProjectBuckets"
    effect = "Allow"

    # s3:* but only on buckets named for this project, including ones that do
    # not exist yet (marts, the web bucket). A bare s3:* on "*" would include
    # the state bucket, and with it the bootstrap state that defines this role.
    actions = ["s3:*"]

    resources = [
      "arn:aws:s3:::${var.project}-*",
      "arn:aws:s3:::${var.project}-*/*",
    ]
  }

  statement {
    sid       = "ListBucketsForTooling"
    effect    = "Allow"
    actions   = ["s3:ListAllMyBuckets", "s3:GetBucketLocation"]
    resources = ["*"]
  }

  # --- 2. IAM, but only for this project ----------------------------------
  statement {
    sid    = "ProjectScopedIam"
    effect = "Allow"

    # Creating Lambda and Glue jobs means creating their execution roles. This
    # is restricted by resource name so the role cannot mint itself a wider one.
    actions = [
      "iam:CreateRole",
      "iam:DeleteRole",
      "iam:GetRole",
      "iam:TagRole",
      "iam:UntagRole",
      "iam:UpdateAssumeRolePolicy",
      "iam:AttachRolePolicy",
      "iam:DetachRolePolicy",
      "iam:PutRolePolicy",
      "iam:DeleteRolePolicy",
      "iam:GetRolePolicy",
      "iam:ListRolePolicies",
      "iam:ListAttachedRolePolicies",
      "iam:PassRole",
      "iam:CreatePolicy",
      "iam:DeletePolicy",
      "iam:GetPolicy",
      "iam:GetPolicyVersion",
      "iam:CreatePolicyVersion",
      "iam:DeletePolicyVersion",
      "iam:ListPolicyVersions",
    ]

    resources = [
      "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.project}-*",
      "arn:aws:iam::${data.aws_caller_identity.current.account_id}:policy/${var.project}-*",
    ]
  }

  statement {
    sid       = "ReadOnlyIamDiscovery"
    effect    = "Allow"
    actions   = ["iam:ListRoles", "iam:ListPolicies", "iam:GetAccountSummary"]
    resources = ["*"]
  }

  # --- 3. the backfill instance exception --------------------------------
  statement {
    sid    = "LaunchBackfillInstanceOnly"
    effect = "Allow"

    actions = [
      "ec2:RunInstances",
      "ec2:CreateTags",
    ]

    resources = ["*"]

    # CLAUDE.md permits one small instance for the backfill and nothing larger.
    # This condition, not the Terraform variable, is what enforces it.
    condition {
      test     = "StringEqualsIfExists"
      variable = "ec2:InstanceType"
      values   = var.backfill_instance_types
    }
  }

  statement {
    sid    = "TerminateAndInspectInstances"
    effect = "Allow"

    # Termination is never restricted: the ability to stop paying for something
    # must not depend on a condition evaluating correctly.
    actions = [
      "ec2:TerminateInstances",
      "ec2:StopInstances",
      "ec2:Describe*",
    ]

    resources = ["*"]
  }

  statement {
    sid    = "BackfillSupportingResources"
    effect = "Allow"

    # RunInstances alone is not enough: the instance needs a security group to
    # sit in and an instance profile to wear.
    actions = [
      "ec2:CreateSecurityGroup",
      "ec2:DeleteSecurityGroup",
      "ec2:AuthorizeSecurityGroupEgress",
      "ec2:RevokeSecurityGroupEgress",
      "ec2:ModifyInstanceAttribute",
      "ec2:ModifyInstanceMetadataOptions",
    ]

    resources = ["*"]
  }

  statement {
    sid    = "ProjectScopedInstanceProfiles"
    effect = "Allow"

    actions = [
      "iam:CreateInstanceProfile",
      "iam:DeleteInstanceProfile",
      "iam:GetInstanceProfile",
      "iam:AddRoleToInstanceProfile",
      "iam:RemoveRoleFromInstanceProfile",
      "iam:TagInstanceProfile",
      "iam:ListInstanceProfilesForRole",
    ]

    resources = [
      "arn:aws:iam::${data.aws_caller_identity.current.account_id}:instance-profile/${var.project}-*",
    ]
  }

  statement {
    sid    = "SsmForAmiLookupAndRemoteCommands"
    effect = "Allow"

    # GetParameter resolves the current Amazon Linux AMI; the rest is how work
    # gets driven on the box without opening an inbound port or holding a key.
    actions = [
      "ssm:GetParameter",
      "ssm:GetParameters",
      "ssm:SendCommand",
      "ssm:GetCommandInvocation",
      "ssm:ListCommandInvocations",
      "ssm:DescribeInstanceInformation",
      "ssm:StartSession",
      "ssm:TerminateSession",
      "ssm:DescribeSessions",
    ]

    resources = ["*"]
  }

  # --- 4. the forbidden list, as an explicit Deny ------------------------
  statement {
    sid    = "DenyForbiddenServices"
    effect = "Deny"

    # Straight from CLAUDE.md. MWAA alone is roughly 350 USD/month against a
    # 30 USD project budget.
    actions = [
      "airflow:*",
      "redshift:*",
      "redshift-serverless:*",
      "redshift-data:*",
      "rds:*",
      "es:*",
      "opensearch:*",
      "aoss:*",
      "elasticache:*",
      "neptune:*",
      "neptune-db:*",
      "sagemaker:*",
      "kinesis:*",
      "elasticmapreduce:RunJobFlow",
      "ec2:CreateNatGateway",
      "ec2:AllocateAddress",
    ]

    resources = ["*"]
  }

  # --- 4a. the bootstrap state is off limits ------------------------------
  statement {
    sid    = "DenyBootstrapState"
    effect = "Deny"

    # ProjectBuckets above matches hype-decay-tfstate-* too, which it must: the
    # deploy role needs the main module state under guardrails/. It has no
    # business in bootstrap/, which holds the state that defines its own
    # permissions. A corrupted bootstrap state is how a constrained role stops
    # being constrained.
    actions   = ["s3:*"]
    resources = ["arn:aws:s3:::${var.project}-tfstate-*/bootstrap/*"]
  }

  # --- 4b. the guardrails cannot be touched by the deployer ---------------
  statement {
    sid    = "DenyGuardrailTampering"
    effect = "Deny"

    # A budget the deployer can delete is a budget on a timer, and a scan limit
    # it can raise is not a limit. These resources are owned by this module and
    # applied by a human; the pipeline identity may read them and nothing else.
    actions = [
      "budgets:ModifyBudget",
      "budgets:DeleteBudget",
      "cloudwatch:DeleteAlarms",
      "cloudwatch:PutMetricAlarm",
      "cloudwatch:DisableAlarmActions",
      "cloudwatch:SetAlarmState",
      "sns:DeleteTopic",
      "sns:SetTopicAttributes",
      "sns:AddPermission",
      "sns:RemovePermission",
      "athena:DeleteWorkGroup",
      "athena:UpdateWorkGroup",
    ]

    resources = [
      "arn:aws:budgets::${data.aws_caller_identity.current.account_id}:budget/${var.project}-monthly",
      "arn:aws:budgets::${data.aws_caller_identity.current.account_id}:budget/${var.project}-tripwire-test",
      "arn:aws:cloudwatch:${var.region}:${data.aws_caller_identity.current.account_id}:alarm:${var.project}-estimated-charges",
      "arn:aws:sns:${var.region}:${data.aws_caller_identity.current.account_id}:${var.project}-alerts",
      "arn:aws:athena:${var.region}:${data.aws_caller_identity.current.account_id}:workgroup/${var.project}",
      "arn:aws:athena:${var.region}:${data.aws_caller_identity.current.account_id}:workgroup/${var.project}-dbt",
    ]
  }

  statement {
    sid    = "DenyUnsubscribingAlerts"
    effect = "Deny"

    # Deliberately unscoped: sns:Unsubscribe takes a SUBSCRIPTION arn, which is
    # generated per subscription and cannot be predicted here. The deploy role
    # has no legitimate reason to unsubscribe anything, so it may unsubscribe
    # nothing.
    actions   = ["sns:Unsubscribe"]
    resources = ["*"]
  }

  # --- 4c. no editing its own permissions --------------------------------
  statement {
    sid    = "DenySelfModification"
    effect = "Deny"

    # ProjectScopedIam above matches hype-decay-*, which includes THIS role.
    # Without this Deny the role could attach itself a wider policy and escape
    # every limit that is not already an explicit Deny -- the allow list would
    # be decorative. Consequence, on purpose: this module is applied with the
    # admin profile, not by the role itself.
    actions = [
      "iam:PutRolePolicy",
      "iam:DeleteRolePolicy",
      "iam:AttachRolePolicy",
      "iam:DetachRolePolicy",
      "iam:UpdateAssumeRolePolicy",
      "iam:DeleteRole",
      "iam:CreatePolicyVersion",
    ]

    resources = [aws_iam_role.deploy.arn]
  }

  # --- 5. region lock ----------------------------------------------------
  statement {
    sid    = "DenyOutsideRegion"
    effect = "Deny"

    # Global services have no request region, so they are exempt or every call
    # to them would be denied.
    not_actions = [
      "iam:*",
      "sts:*",
      "s3:*",
      "budgets:*",
      "ce:*",
      "cur:*",
      "account:*",
      "organizations:*",
      "support:*",
      "cloudfront:*",
      "route53:*",
    ]

    resources = ["*"]

    condition {
      test     = "StringNotEquals"
      variable = "aws:RequestedRegion"
      values   = [var.region]
    }
  }
}

resource "aws_iam_role_policy" "deploy" {
  name   = "${var.project}-deploy"
  role   = aws_iam_role.deploy.id
  policy = data.aws_iam_policy_document.deploy.json
}
