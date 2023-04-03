####################################
### extraction lambda monitoring ###
####################################

# resource "aws_cloudwatch_metric_alarm" "lambda_duration_alarm" {
#   alarm_name          = "lambda_duration_alarm"
#   comparison_operator = "GreaterThanOrEqualToThreshold"
#   evaluation_periods  = "1"
#   metric_name         = "Duration"
#   namespace           = "AWS/Lambda"
#   period              = "60"
#   statistic           = "Average"
#   threshold           = "600"
#   alarm_description   = "This alarm triggers when the Lambda function takes longer than 600ms to execute."

#   alarm_actions = [aws_sns_topic.error_alert.arn]

# creates a metric filter for errors in extraction lambda log group
resource "aws_cloudwatch_log_metric_filter" "errors_filter" {
  name           = "error_filter"
  pattern        = "ERROR"
  log_group_name = aws_cloudwatch_log_group.extraction_lambda_log.name

  metric_transformation {
    name      = "ErrorCount"
    namespace = "CustomMetrics"
    value     = "1"
  }
}

# creates a cw metric alarm for the extraction lambda based on the metric filter above
resource "aws_cloudwatch_metric_alarm" "extraction_lambda_errors_alarm" {
  alarm_name          = "extraction-lambda-error-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  metric_name         = aws_cloudwatch_log_metric_filter.errors_filter.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.errors_filter.metric_transformation[0].namespace
  alarm_description   = "This metric monitors error count."

  # this will trigger a sns topic defined in the sns module
  alarm_actions = [aws_sns_topic.error_alerts.arn]
}

###################################
### transform lambda monitoring ###
###################################

# creates a metric filter for errors in transform lambda log group
resource "aws_cloudwatch_log_metric_filter" "transform_errors_filter" {
  name           = "error_filter"
  pattern        = "ERROR"
  log_group_name = aws_cloudwatch_log_group.transform_lambda_log.name

  metric_transformation {
    name      = "ErrorCount"
    namespace = "CustomMetrics"
    value     = "1"
  }
}

# creates a cw metric alarm for the transform lambda based on the metric filter above
resource "aws_cloudwatch_metric_alarm" "transform_lambda_errors_alarm" {
  alarm_name          = "transform-lambda-error-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  metric_name         = aws_cloudwatch_log_metric_filter.transform_errors_filter.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.transform_errors_filter.metric_transformation[0].namespace
  alarm_description   = "This metric monitors error count."

  # this will trigger a sns topic defined in the sns module
  alarm_actions = [aws_sns_topic.error_alerts.arn]
}

##############################
### load lambda monitoring ###
##############################

# creates a metric filter for errors in load lambda log group
resource "aws_cloudwatch_log_metric_filter" "load_errors_filter" {
  name           = "error_filter"
  pattern        = "ERROR"
  log_group_name = aws_cloudwatch_log_group.load_lambda_log.name

  metric_transformation {
    name      = "ErrorCount"
    namespace = "CustomMetrics"
    value     = "1"
  }
}

# creates a cw metric alarm for the load lambda based on the metric filter above
resource "aws_cloudwatch_metric_alarm" "load_lambda_errors_alarm" {
  alarm_name          = "load-lambda-error-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  metric_name         = aws_cloudwatch_log_metric_filter.load_errors_filter.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.load_errors_filter.metric_transformation[0].namespace
  alarm_description   = "This metric monitors error count."

  # this will trigger a sns topic defined in the sns module
  alarm_actions = [aws_sns_topic.error_alerts.arn]
}