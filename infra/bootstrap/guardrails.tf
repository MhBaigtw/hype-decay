# ---------------------------------------------------------------------------
# The cost guardrails live here, not in the main module.
#
# Reason: the main module is applied by the deploy role, and a guardrail the
# deployer can delete is a guardrail on a timer. Moving budgets, the billing
# alarm and the alert topic into the admin-only module -- and denying the deploy
# role delete/modify on each of them, see iam.tf -- means an error in pipeline
# code cannot remove the thing that would report the error.
#
# Two independent tripwires, on purpose:
#   1. AWS Budgets watches actual month-to-date spend, billing-side.
#   2. A CloudWatch alarm watches AWS/Billing EstimatedCharges, metric-side.
# If one is misconfigured the other still fires.
# ---------------------------------------------------------------------------

resource "aws_sns_topic" "alerts" {
  name         = "${var.project}-alerts"
  display_name = "hype-decay cost alerts"
}

resource "aws_sns_topic_subscription" "alerts_email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.contact_email

  # NOT imported: the original subscription was never confirmed and AWS has
  # since discarded it, leaving the topic with zero subscribers and the billing
  # alarm publishing into nothing. Applying this module creates it again, which
  # sends a fresh confirmation email. Terraform cannot confirm it for you; until
  # the link is clicked this stays PendingConfirmation and the alarm leg is dead.
}

data "aws_iam_policy_document" "alerts_topic" {
  statement {
    sid    = "AllowBudgetsPublish"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["budgets.amazonaws.com"]
    }

    actions   = ["SNS:Publish"]
    resources = [aws_sns_topic.alerts.arn]

    # Without this condition any AWS account could aim its budgets at our topic.
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }

  statement {
    sid    = "AllowCloudWatchAlarmsPublish"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }

    actions   = ["SNS:Publish"]
    resources = [aws_sns_topic.alerts.arn]

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }

  statement {
    sid    = "AllowAccountOwnerManagement"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }

    actions = [
      "SNS:Publish",
      "SNS:Subscribe",
      "SNS:GetTopicAttributes",
      "SNS:SetTopicAttributes",
      "SNS:ListSubscriptionsByTopic",
      "SNS:AddPermission",
      "SNS:RemovePermission",
      "SNS:DeleteTopic",
    ]

    resources = [aws_sns_topic.alerts.arn]
  }
}

resource "aws_sns_topic_policy" "alerts" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.alerts_topic.json
}

# --- tripwire 1: the real budget ------------------------------------------

resource "aws_budgets_budget" "monthly" {
  name         = "${var.project}-monthly"
  budget_type  = "COST"
  limit_amount = var.budget_limit_usd
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  dynamic "notification" {
    for_each = var.budget_alert_percentages

    content {
      comparison_operator = "GREATER_THAN"
      threshold           = notification.value
      threshold_type      = "PERCENTAGE"
      notification_type   = "ACTUAL"

      # Both a direct email and the topic. The direct address does not depend on
      # the SNS subscription being confirmed, which is why budget alerts still
      # arrive while the alarm leg is dead.
      subscriber_email_addresses = [var.contact_email]
      subscriber_sns_topic_arns  = [aws_sns_topic.alerts.arn]
    }
  }

  depends_on = [aws_sns_topic_policy.alerts]
}

# --- tripwire 1b: proof the alert path delivers ---------------------------

resource "aws_budgets_budget" "tripwire_test" {
  count = var.create_tripwire_test_budget ? 1 : 0

  name         = "${var.project}-tripwire-test"
  budget_type  = "COST"
  limit_amount = "0.01"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # Any spend at all breaches this, so the notification fires on the next budget
  # evaluation instead of waiting for 5 USD of real spend. Still unproven as of
  # 2026-09-30: month-to-date cost reads -0.0000 USD, so there has been nothing
  # to breach. Inconclusive, not failed.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.contact_email]
    subscriber_sns_topic_arns  = [aws_sns_topic.alerts.arn]
  }

  depends_on = [aws_sns_topic_policy.alerts]
}

# --- tripwire 2: CloudWatch billing alarm ---------------------------------

resource "aws_cloudwatch_metric_alarm" "billing" {
  alarm_name        = "${var.project}-estimated-charges"
  alarm_description = "Estimated charges crossed ${var.billing_alarm_threshold_usd} USD. Independent of AWS Budgets on purpose."

  namespace   = "AWS/Billing"
  metric_name = "EstimatedCharges"
  dimensions  = { Currency = "USD" }

  statistic           = "Maximum"
  period              = 21600 # 6h: the metric only updates a few times a day
  evaluation_periods  = 1
  threshold           = var.billing_alarm_threshold_usd
  comparison_operator = "GreaterThanOrEqualToThreshold"

  # Missing data must not read as "fine".
  treat_missing_data = "missing"

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
}
