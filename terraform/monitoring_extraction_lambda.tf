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



resource "aws_cloudwatch_log_metric_filter" "errors_filter" {
  name           = "error_filter"
  pattern        = "ERROR"
  log_group_name = aws_cloudwatch_log_group.ingestion_lambda_log.name

  metric_transformation {
    name      = "ErrorCount"
    namespace = "CustomMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "ingestion_lambda_errors_alarm" {
  alarm_name = "ingestion-lambda-error-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  period = "60"
  statistic = "Sum"
  threshold = "1"
  metric_name =  aws_cloudwatch_log_metric_filter.errors_filter.metric_transformation[0].name
  namespace = aws_cloudwatch_log_metric_filter.errors_filter.metric_transformation[0].namespace
  alarm_description   = "This metric monitors error count."

  alarm_actions = [aws_sns_topic.error_alerts.arn]

  dimensions = {
  FunctionName = resource.aws_lambda_function.ingestion_lambda.name
  }
}