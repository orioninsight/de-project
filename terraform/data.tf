# gets current account id
data "aws_caller_identity" "current" {}

# gets current region
data "aws_region" "current" {}

# zips up extraction deployment package created by make lambda-deployment-packages
data "archive_file" "extraction_lambda_archive" {
  type             = "zip"
  source_dir      = "${path.module}/../archives/extraction_lambda"
  output_path      = "${path.module}/../archives/extraction_lambda.zip"
}

# zips up transform package created by make lambda-deployment-packages
data "archive_file" "transform_lambda_archive" {
  type             = "zip"
  source_dir      = "${path.module}/../archives/transform_lambda"
  output_path      = "${path.module}/../archives/transform_lambda.zip"
}

# zips up load deployment package created by make lambda-deployment-packages
data "archive_file" "load_lambda_archive" {
  type             = "zip"
  source_dir      = "${path.module}/../archives/load_lambda"
  output_path      = "${path.module}/../archives/load_lambda.zip"
}

# allows use of predefined db credentials secret
data "aws_secretsmanager_secret" "database_secret" {
  name = "OI_TOTESYS_DB_INFO"
}


