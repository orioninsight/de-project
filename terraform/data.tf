data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "local_file" "ingestion_lambda_archive" {
  filename = "${path.module}/../archives/extraction_lambda.zip"
}

data "aws_secretsmanager_secret" "database_secret" {
  name = "totesys_db"
}
