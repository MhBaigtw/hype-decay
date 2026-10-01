# ---------------------------------------------------------------------------
# Handover: these resources moved to infra/bootstrap and are now managed there.
#
# `removed` with `lifecycle { destroy = false }` means FORGET, not DELETE.
# Terraform drops the resource from this module state and leaves the live object
# alone. Without the lifecycle block -- or by simply deleting the resource
# blocks, which is what already happened to iam.tf -- a plan here would schedule
# the deploy role for destruction while running AS the deploy role.
#
# Order matters. Apply infra/bootstrap FIRST so its import blocks adopt these
# objects, then apply this module so they are forgotten here. Between the two
# applies both states reference the same live objects, which is harmless as long
# as only one of them is applied at a time.
#
# Two resources have no counterpart import in bootstrap:
#   * aws_sns_topic_subscription.alerts_email -- the live subscription is gone,
#     never confirmed, so bootstrap creates a fresh one. This block only drops
#     the stale state entry.
#   * aws_athena_workgroup.dbt was never applied from here, so there is nothing
#     to remove.
#
# Once both applies have landed, this file and bootstrap/imports.tf are inert
# and should be deleted in a follow-up commit.
# ---------------------------------------------------------------------------

removed {
  from = aws_iam_role.deploy

  lifecycle {
    destroy = false
  }
}

removed {
  from = aws_iam_role_policy.deploy

  lifecycle {
    destroy = false
  }
}

removed {
  from = aws_sns_topic.alerts

  lifecycle {
    destroy = false
  }
}

removed {
  from = aws_sns_topic_policy.alerts

  lifecycle {
    destroy = false
  }
}

removed {
  from = aws_sns_topic_subscription.alerts_email

  lifecycle {
    destroy = false
  }
}

removed {
  from = aws_budgets_budget.monthly

  lifecycle {
    destroy = false
  }
}

removed {
  from = aws_budgets_budget.tripwire_test

  lifecycle {
    destroy = false
  }
}

removed {
  from = aws_cloudwatch_metric_alarm.billing

  lifecycle {
    destroy = false
  }
}

removed {
  from = aws_athena_workgroup.main

  lifecycle {
    destroy = false
  }
}
