resource "aws_cloudwatch_log_group" "ingestion_lambda_log" {
  name = "/aws/lambda/extraction_lambda"

  tags = {
    Application = "totes"
  }
}
