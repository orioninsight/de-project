# creates a sns topic for lambda error alerts
resource "aws_sns_topic" "error_alerts" {
  name = "extraction-lambda-error-alerts"
}

# adds a email endpoint to the sns topic using email varaible defined in vars
resource "aws_sns_topic_subscription" "lambda_alert_email" {
  topic_arn = aws_sns_topic.error_alerts.arn
  protocol  = "email"
  endpoint  = var.email
}
