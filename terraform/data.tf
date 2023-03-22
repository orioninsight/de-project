data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "archive_file" "ingestion_lambda_archive" {
  type        = "zip"
  source_dir = "${path.module}/../src/ingestion_lambda/app"
  output_path = "${path.module}/../archives/ingestion_lambda/app.zip"
}

