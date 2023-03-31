resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = var.code_bucket_prefix
}

resource "aws_s3_bucket" "ingestion_zone_bucket" {
  bucket_prefix = var.ingestion_zone_bucket_prefix
}

resource "aws_s3_bucket" "transformed_zone_bucket" {
  bucket_prefix = var.transformed_zone_bucket_prefix
}

resource "aws_s3_object" "ingestion_lambda_code" {
  key         = "extraction_lambda/extraction_lambda.zip"
  source      = data.local_file.ingestion_lambda_archive.filename
  bucket      = aws_s3_bucket.code_bucket.bucket
  source_hash = filebase64sha256(data.local_file.ingestion_lambda_archive.filename)
  depends_on = [
    data.local_file.ingestion_lambda_archive
  ]
}

resource "aws_s3_object" "transform_lambda_code" {
  key         = "transform_lambda/transform_lambda.zip"
  source      = data.local_file.transform_lambda_archive.filename
  bucket      = aws_s3_bucket.code_bucket.bucket
  source_hash = filebase64sha256(data.local_file.transform_lambda_archive.filename)
  depends_on = [
    data.local_file.transform_lambda_archive
  ]
}


