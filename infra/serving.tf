# ---------------------------------------------------------------------------
# Task 7: the serving layer. The site never queries Athena.
#
# A single-spike query against the dbt models costs about $0.10 (Task 5: the
# filter does not reach the table scans), so the public page reads a small
# precomputed DynamoDB table instead, loaded from fct_half_life by
# scripts/load_serving.py. A Lambda behind an API Gateway HTTP API serves it.
#
# Standing cost with zero traffic: DynamoDB storage only (tens of MB, cents a
# month). Lambda, API Gateway and on-demand DynamoDB bill per request.
# ---------------------------------------------------------------------------

resource "aws_dynamodb_table" "serving" {
  name         = "${var.project}-serving"
  billing_mode = "PAY_PER_REQUEST" # CLAUDE.md: DynamoDB on-demand only
  hash_key     = "pk"
  range_key    = "sk"
  tags         = { Task = "task-7-serving" }

  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "S"
  }
  attribute {
    name = "gsi1pk"
    type = "S"
  }
  attribute {
    name = "gsi1sk"
    type = "S"
  }

  # Title search: partition = first character of the normalised title, sort =
  # normalised title + spike start, so a prefix search is one Query with
  # begins_with. Only the fields a search result shows are projected, so the
  # curves never ride along.
  global_secondary_index {
    name               = "by_title"
    hash_key           = "gsi1pk"
    range_key          = "gsi1sk"
    projection_type    = "INCLUDE"
    non_key_attributes = ["page_title", "spike_start", "attention_half_life_hours", "peak_day_views", "window_end"]
  }
}

# --- the API's identity: read the serving table, write its own logs -------

data "aws_iam_policy_document" "api_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "api" {
  name               = "${var.project}-api"
  description        = "Public read API. Reads the serving table and nothing else."
  assume_role_policy = data.aws_iam_policy_document.api_assume.json
  tags               = { Task = "task-7-serving" }
}

data "aws_iam_policy_document" "api" {
  statement {
    sid       = "ReadServingTableOnly"
    effect    = "Allow"
    actions   = ["dynamodb:GetItem", "dynamodb:BatchGetItem", "dynamodb:Query"]
    resources = [aws_dynamodb_table.serving.arn, "${aws_dynamodb_table.serving.arn}/index/*"]
  }
  statement {
    sid       = "OwnLogs"
    effect    = "Allow"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.api.arn}:*"]
  }
}

resource "aws_iam_role_policy" "api" {
  name   = "${var.project}-api"
  role   = aws_iam_role.api.id
  policy = data.aws_iam_policy_document.api.json
}

resource "aws_cloudwatch_log_group" "api" {
  name              = "/aws/lambda/${var.project}-api"
  retention_in_days = 14 # logs are for debugging, not history
  tags              = { Task = "task-7-serving" }
}

# --- the function ----------------------------------------------------------

data "archive_file" "api" {
  type        = "zip"
  source_file = "${path.module}/../api/handler.py"
  output_path = "${path.module}/.build/api.zip"
}

resource "aws_lambda_function" "api" {
  function_name    = "${var.project}-api"
  role             = aws_iam_role.api.arn
  runtime          = "python3.12"
  architectures    = ["arm64"]
  handler          = "handler.handler"
  filename         = data.archive_file.api.output_path
  source_code_hash = data.archive_file.api.output_base64sha256
  memory_size      = 256
  timeout          = 5
  tags             = { Task = "task-7-serving" }

  environment {
    variables = {
      TABLE_NAME = aws_dynamodb_table.serving.name
    }
  }

  depends_on = [aws_cloudwatch_log_group.api]
}

# --- the HTTP API ------------------------------------------------------------

resource "aws_apigatewayv2_api" "public" {
  name          = "${var.project}-public"
  protocol_type = "HTTP"
  description   = "Read-only API for the public page."
  tags          = { Task = "task-7-serving" }

  # The page is on Netlify, a different origin, so the browser needs CORS.
  # GET only, no credentials.
  cors_configuration {
    allow_origins = var.site_origins
    allow_methods = ["GET"]
    allow_headers = ["content-type"]
    max_age       = 3600
  }
}

resource "aws_apigatewayv2_integration" "api" {
  api_id                 = aws_apigatewayv2_api.public.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "api" {
  # The spike id is a query parameter: titles contain "/" (AC/DC).
  for_each  = toset(["GET /api/summary", "GET /api/search", "GET /api/spike"])
  api_id    = aws_apigatewayv2_api.public.id
  route_key = each.value
  target    = "integrations/${aws_apigatewayv2_integration.api.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.public.id
  name        = "$default"
  auto_deploy = true
  tags        = { Task = "task-7-serving" }

  # Throttling for the whole API, so a crawler cannot run up a bill. HTTP APIs
  # have no per-client limit without WAF (a standing cost), so this is a
  # global ceiling: requests beyond it get a 429 and cost nothing downstream.
  # At the ceiling, sustained for a whole month, the bill is bounded (NOTES,
  # Task 7) -- and the budget alerts fire long before that.
  default_route_settings {
    throttling_rate_limit  = var.api_rate_limit
    throttling_burst_limit = var.api_burst_limit
  }
}

resource "aws_lambda_permission" "api" {
  statement_id  = "AllowHttpApi"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.public.execution_arn}/*/*"
}

output "api_url" {
  description = "Base URL of the public read API."
  value       = aws_apigatewayv2_api.public.api_endpoint
}

output "serving_table" {
  description = "DynamoDB table the site reads."
  value       = aws_dynamodb_table.serving.name
}

# --- runaway-traffic alarm -------------------------------------------------
#
# The stage throttle is best-effort: set to 1 request/second, it accepted 7.4/s
# under a measured 30-second flood (AWS applies it per node). Sustained for a
# month that is about $50. A per-client limit needs WAF, a standing cost, and
# CloudFront is not an allowed service (CLAUDE.md), so the backstop is speed of
# DETECTION: this alarm emails within the hour instead of the budget emailing
# days later. 20,000 requests in an hour (~5.6/s) is far above any real use of
# this page. Missing data is fine here: no traffic is not a problem.
resource "aws_cloudwatch_metric_alarm" "api_runaway" {
  alarm_name          = "${var.project}-api-runaway-traffic"
  alarm_description   = "Public API passed 20,000 requests in an hour: likely a crawler. Consider disabling the stage."
  namespace           = "AWS/ApiGateway"
  metric_name         = "Count"
  dimensions          = { ApiId = aws_apigatewayv2_api.public.id }
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 20000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  # Email, AND trip the kill switch (an alarm Lambda action).
  alarm_actions = [
    "arn:aws:sns:${var.region}:${data.aws_caller_identity.current.account_id}:${var.project}-alerts",
    aws_lambda_function.killswitch.arn,
  ]
  ok_actions = ["arn:aws:sns:${var.region}:${data.aws_caller_identity.current.account_id}:${var.project}-alerts"]
  tags       = { Task = "task-7-serving" }
}

# --- automatic kill switch -------------------------------------------------
#
# When the runaway alarm fires it invokes this Lambda directly (an alarm Lambda
# action). It sets the API Lambda's reserved concurrency to 0 -- every API
# request is then refused, and nothing behind it runs or bills -- and emails
# what it did and the one-command restore (api/killswitch.py):
#
#   aws lambda delete-function-concurrency --function-name hype-decay-api --profile hype-decay-deploy
#
# A `terraform plan` while it is tripped shows reserved_concurrent_executions
# 0 -> -1 on aws_lambda_function.api: applying that restores it too.

data "archive_file" "killswitch" {
  type        = "zip"
  source_file = "${path.module}/../api/killswitch.py"
  output_path = "${path.module}/.build/killswitch.zip"
}

resource "aws_iam_role" "killswitch" {
  name               = "${var.project}-api-killswitch"
  description        = "May switch the public API Lambda off, and say so. Nothing else."
  assume_role_policy = data.aws_iam_policy_document.api_assume.json
  tags               = { Task = "task-6-daily" }
}

data "aws_iam_policy_document" "killswitch" {
  statement {
    sid       = "SwitchOffTheApiOnly"
    effect    = "Allow"
    actions   = ["lambda:PutFunctionConcurrency"]
    resources = [aws_lambda_function.api.arn]
  }
  statement {
    sid       = "SayWhatItDid"
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = ["arn:aws:sns:${var.region}:${data.aws_caller_identity.current.account_id}:${var.project}-alerts"]
  }
  statement {
    sid       = "OwnLogs"
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.project}-api-killswitch:*"]
  }
}

resource "aws_iam_role_policy" "killswitch" {
  name   = "${var.project}-api-killswitch"
  role   = aws_iam_role.killswitch.id
  policy = data.aws_iam_policy_document.killswitch.json
}

resource "aws_cloudwatch_log_group" "killswitch" {
  name              = "/aws/lambda/${var.project}-api-killswitch"
  retention_in_days = 90 # rare events; keep the record longer
  tags              = { Task = "task-6-daily" }
}

resource "aws_lambda_function" "killswitch" {
  function_name    = "${var.project}-api-killswitch"
  role             = aws_iam_role.killswitch.arn
  runtime          = "python3.12"
  architectures    = ["arm64"]
  handler          = "killswitch.handler"
  filename         = data.archive_file.killswitch.output_path
  source_code_hash = data.archive_file.killswitch.output_base64sha256
  memory_size      = 128
  timeout          = 10
  tags             = { Task = "task-6-daily" }

  environment {
    variables = {
      API_FUNCTION    = aws_lambda_function.api.function_name
      ALERT_TOPIC_ARN = "arn:aws:sns:${var.region}:${data.aws_caller_identity.current.account_id}:${var.project}-alerts"
      ALARM_NAME      = "${var.project}-api-runaway-traffic"
    }
  }

  depends_on = [aws_cloudwatch_log_group.killswitch]
}

resource "aws_lambda_permission" "killswitch" {
  statement_id  = "AllowRunawayAlarm"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.killswitch.function_name
  principal     = "lambda.alarms.cloudwatch.amazonaws.com"
  source_arn    = aws_cloudwatch_metric_alarm.api_runaway.arn
}
