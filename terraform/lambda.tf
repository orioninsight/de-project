resource "aws_lambda_function" "ingestion_lambda" {
  function_name    = var.ingestion_lambda_name
  role             = aws_iam_role.ingestion_lambda_role.arn
  handler          = var.ingestion_lambda_handler
  runtime          = "python3.9"
  source_code_hash = filebase64sha256(data.archive_file.ingestion_lambda_archive.output_path)

  // Here's where we specify the code location
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key    = aws_s3_object.ingestion_lambda_code.key

  depends_on = [
    aws_s3_object.ingestion_lambda_code
  ]
}


