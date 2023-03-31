resource "aws_cloudwatch_log_metric_filter" "transform_errors_filter" {
  name           = "error_filter"
  pattern        = "ERROR"
  log_group_name = aws_cloudwatch_log_group.transform_lambda_log.name

  metric_transform {
    name      = "ErrorCount"
    namespace = "CustomMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "transform_lambda_errors_alarm" {
  alarm_name = "transform-lambda-error-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  period = "60"
  statistic = "Sum"
  threshold = "1"
  metric_name =  aws_cloudwatch_log_metric_filter.transform_errors_filter.metric_transform[0].name
  namespace = aws_cloudwatch_log_metric_filter.transform_errors_filter.metric_transform[0].namespace
  alarm_description   = "This metric monitors error count."

  alarm_actions = [aws_sns_topic.error_alerts.arn]
}