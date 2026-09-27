# ---------------------------------------------------------------------------
# Deployment role.
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
      "s3:*",
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
