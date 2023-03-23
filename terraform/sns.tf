resource "aws_sns_topic" "error_alerts" {
  name = "ingestion-lambda-error-alerts"
}

resource "aws_sns_topic_subscription" "lambda_alert_email" {
  topic_arn = aws_sns_topic.error_alerts.arn
  protocol  = "email"
  endpoint  = var.email
}