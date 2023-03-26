resource "aws_lambda_function" "ingestion_lambda" {
  function_name    = "extraction_lambda"
  role             = aws_iam_role.ingestion_lambda_role.arn
  handler          = var.extraction_lambda_handler
  runtime          = "python3.9"
  source_code_hash = filebase64sha256(data.local_file.ingestion_lambda_archive.filename)
  timeout          = 30
  memory_size      = 192

  // Here's where we specify the code location
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key    = aws_s3_object.ingestion_lambda_code.key

  depends_on = [
    aws_s3_object.ingestion_lambda_code
  ]
  environment {
    variables = {
      OI_STORER_SECRET_STRING = jsonencode({ "s3_bucket_name" : "${aws_s3_bucket.ingestion_zone_bucket.id}" })
    }
  }
}

resource "aws_cloudwatch_event_rule" "scheduler" {
  name_prefix         = "ingestion-scheduler-"
  schedule_expression = "rate(1 minute)"
}

resource "aws_lambda_permission" "allow_scheduler" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.ingestion_lambda.function_name
  principal      = "events.amazonaws.com"
  source_arn     = aws_cloudwatch_event_rule.scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule = aws_cloudwatch_event_rule.scheduler.name
  arn  = aws_lambda_function.ingestion_lambda.arn
}
