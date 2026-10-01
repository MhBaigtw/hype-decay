# ---------------------------------------------------------------------------
# Config-driven import: adopt resources that already exist in AWS and are
# currently tracked by the MAIN module state.
#
# The handover is declarative on both sides and runs in this order:
#
#   1. apply THIS module. The import blocks below adopt the live resources into
#      bootstrap state. Nothing is created, changed or destroyed by an import.
#   2. apply the main module. Its `removed` blocks (infra/removed.tf) drop the
#      same resources from main state with `destroy = false`, so they are
#      forgotten, not deleted.
#
# Between the two applies both states reference the same objects. That is safe
# as long as nobody applies main in between -- which is exactly why main must
# not be planned or applied until step 1 is done: main no longer DECLARES these
# resources, so a plan there today would schedule the deploy role for
# destruction while running as the deploy role.
#
# After both applies land, these import blocks and the removed blocks are inert
# and can be deleted in a later commit.
#
# Two resources are deliberately absent here:
#   * aws_sns_topic_subscription.alerts_email -- the live subscription is gone
#     (never confirmed, so AWS discarded it). It is created fresh instead.
#   * aws_athena_workgroup.dbt -- new, never applied anywhere.
# ---------------------------------------------------------------------------

import {
  to = aws_iam_role.deploy
  id = "hype-decay-deploy"
}

import {
  to = aws_iam_role_policy.deploy
  id = "hype-decay-deploy:hype-decay-deploy"
}

import {
  to = aws_sns_topic.alerts
  id = "arn:aws:sns:us-east-1:820697996849:hype-decay-alerts"
}

import {
  to = aws_sns_topic_policy.alerts
  id = "arn:aws:sns:us-east-1:820697996849:hype-decay-alerts"
}

import {
  to = aws_budgets_budget.monthly
  id = "820697996849:hype-decay-monthly"
}

import {
  to = aws_budgets_budget.tripwire_test[0]
  id = "820697996849:hype-decay-tripwire-test"
}

import {
  to = aws_cloudwatch_metric_alarm.billing
  id = "hype-decay-estimated-charges"
}

import {
  to = aws_athena_workgroup.main
  id = "hype-decay"
}
