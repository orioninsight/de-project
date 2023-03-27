resource "aws_cloudwatch_log_group" "ingestion_lambda_log" {
  name = "/aws/lambda/extraction_lambda"

  tags = {
    Application = "totes"
  }
}

resource "aws_cloudwatch_log_group" "transformation_lambda_log" {
  name = "/aws/lambda/transformation_lambda"

  tags = {
    Application = "totes"
  }
}