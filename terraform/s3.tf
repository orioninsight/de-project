# creates bucket to house code for lambdas
resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = var.code_bucket_prefix
}

# creates bucket for data extracted from database
resource "aws_s3_bucket" "ingestion_zone_bucket" {
  bucket_prefix = var.ingestion_zone_bucket_prefix
}

# creates bucket for remodelled extracted data
resource "aws_s3_bucket" "transformed_zone_bucket" {
  bucket_prefix = var.transformed_zone_bucket_prefix
}

# creates s3 object from extraction deployment package
resource "aws_s3_object" "extraction_lambda_code" {
  key         = "extraction_lambda/extraction_lambda.zip"
  source      = data.local_file.extraction_lambda_archive.filename
  bucket      = aws_s3_bucket.code_bucket.bucket
  source_hash = filebase64sha256(data.local_file.extraction_lambda_archive.filename)
  depends_on = [
    data.local_file.extraction_lambda_archive
  ]
}

# creates s3 object from transform deployment package
resource "aws_s3_object" "transform_lambda_code" {
  key         = "transform_lambda/transform_lambda.zip"
  source      = data.local_file.transform_lambda_archive.filename
  bucket      = aws_s3_bucket.code_bucket.bucket
  source_hash = filebase64sha256(data.local_file.transform_lambda_archive.filename)
  depends_on = [
    data.local_file.transform_lambda_archive
  ]
}

# creates s3 object from load deployment package
resource "aws_s3_object" "load_lambda_code" {
  key         = "load_lambda/load_lambda.zip"
  source      = data.local_file.load_lambda_archive.filename
  bucket      = aws_s3_bucket.code_bucket.bucket
  source_hash = filebase64sha256(data.local_file.load_lambda_archive.filename)
  depends_on = [
    data.local_file.load_lambda_archive
  ]
}



