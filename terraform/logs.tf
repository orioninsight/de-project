resource "aws_cloudwatch_log_group" "ingestion_lambda_log" {
  name = "/aws/lambda/${var.ingestion_lambda_name}"

  tags = {
    Application = "totes"
  }
}
