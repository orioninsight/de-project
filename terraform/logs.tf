# creates a log group for extraction lambda
resource "aws_cloudwatch_log_group" "extraction_lambda_log" {
  name = "/aws/lambda/extraction_lambda"

  tags = {
    Application = "totes"
  }
}

# creates a log group for transform lambda
resource "aws_cloudwatch_log_group" "transform_lambda_log" {
  name = "/aws/lambda/transform_lambda"

  tags = {
    Application = "totes"
  }
}