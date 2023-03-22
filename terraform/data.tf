data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "archive_file" "ingestion_lambda_archive" {
  type        = "zip"
  source_file = "${path.module}/../src/ingestion_lambda/${var.ingestion_lambda_name}.py"
  output_path = "${path.module}/../${var.ingestion_lambda_name}.zip"
}

