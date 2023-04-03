# gets current account id
data "aws_caller_identity" "current" {}

# gets current region
data "aws_region" "current" {}

# refers to extraction deployment package created by make lambda-deployment-packages
data "local_file" "extraction_lambda_archive" {
  filename = "${path.module}/../archives/extraction_lambda.zip"
}

# refers to transform deployment package created by make lambda-deployment-packages
data "local_file" "transform_lambda_archive" {
  filename = "${path.module}/../archives/transform_lambda.zip"
}

# refers to load deployment package created by make lambda-deployment-packages
data "local_file" "load_lambda_archive" {
  filename = "${path.module}/../archives/load_lambda.zip"
}

# allows use of predefined db credentials secret
data "aws_secretsmanager_secret" "database_secret" {
  name = "OI_TOTESYS_DB_INFO"
}
