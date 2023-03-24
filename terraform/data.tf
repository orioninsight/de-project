data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "archive_file" "ingestion_lambda_archive" {
  type        = "zip"
  source_dir  = "${path.module}/../src/extraction_lambda"
  output_path = "${path.module}/../archives/extraction_lambda/app.zip"
}

