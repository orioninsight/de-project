#########################
### extraction lambda ###
#########################

# specifies the IAM role to be associated with the Lambda function using the aws_iam_role resource.
# specifies the runtime environment and source code for the Lambda function.
resource "aws_lambda_function" "extraction_lambda" {
  function_name    = "extraction_lambda"
  role             = aws_iam_role.extraction_lambda_role.arn
  handler          = var.extraction_lambda_handler
  runtime          = "python3.9"
  source_code_hash = filebase64sha256(data.local_file.extraction_lambda_archive.filename)
  timeout          = 30
  memory_size      = 192

  // Here's where we specify the code location
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key    = aws_s3_object.extraction_lambda_code.key

  depends_on = [
    aws_s3_object.extraction_lambda_code, aws_lambda_function.transform_lambda
  ]

  # sets ingestion bucket and tranform lambda arn as environemental variables
  environment {
    variables = {
      OI_STORER_INFO           = jsonencode({ "s3_bucket_name" : "${aws_s3_bucket.ingestion_zone_bucket.id}" })
      OI_TRANSFORM_LAMBDA_INFO = jsonencode({ "transform_lambda_arn" : "${aws_lambda_function.transform_lambda.arn}" })
    }
  }
}


# defines a CloudWatch event rule which runs every minute
resource "aws_cloudwatch_event_rule" "scheduler" {
  name_prefix         = "extraction-scheduler-"
  schedule_expression = "rate(1 minute)"
}

# gives permission for the events.amazonaws.com principal to invoke the extraction_lambda function in response to the scheduler rule
resource "aws_lambda_permission" "allow_scheduler" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.extraction_lambda.function_name
  principal      = "events.amazonaws.com"
  source_arn     = aws_cloudwatch_event_rule.scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}

# links the extraction_lambda function with the scheduler rule as a target
resource "aws_cloudwatch_event_target" "lambda_target" {
  rule = aws_cloudwatch_event_rule.scheduler.name
  arn  = aws_lambda_function.extraction_lambda.arn
}

########################
### transform lambda ###
########################

# specifies the IAM role to be associated with the Lambda function using the aws_iam_role resource.
# specifies the runtime environment and source code for the Lambda function.
resource "aws_lambda_function" "transform_lambda" {
  function_name    = "transform_lambda"
  role             = aws_iam_role.transform_lambda_role.arn
  handler          = var.transform_lambda_handler
  runtime          = "python3.9"
  source_code_hash = filebase64sha256(data.local_file.transform_lambda_archive.filename)
  timeout          = 30
  memory_size      = 192

  # Here's where we specify the code location
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key    = aws_s3_object.transform_lambda_code.key

  # makes sure object is created first
  depends_on = [
    aws_s3_object.transform_lambda_code
  ]

  # set extracted and processed bucket names as environmental variables
  environment {
    variables = {
      OI_STORER_INFO    = jsonencode({ "s3_bucket_name" : "${aws_s3_bucket.ingestion_zone_bucket.id}" }),
      OI_PROCESSED_INFO = jsonencode({ "s3_bucket_name" : "${aws_s3_bucket.transformed_zone_bucket.id}" })
    }
  }
}


# gives permission for the lambda.amazonaws.com principal to invoke the transform_lambda function in response to the extraction_lambda function
resource "aws_lambda_permission" "allow_extraction_lambda" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transform_lambda.function_name
  principal     = "lambda.amazonaws.com"

  source_arn = aws_lambda_function.extraction_lambda.arn
}

