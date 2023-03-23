resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = var.code_bucket_prefix
}

resource "aws_s3_bucket" "ingestion_zone_bucket" {
  bucket_prefix = var.ingestion_zone_bucket_prefix
}

resource "aws_s3_object" "ingestion_lambda_code" {
  key    = "extraction_lambda/app.zip"
  source = data.archive_file.ingestion_lambda_archive.output_path
  bucket = aws_s3_bucket.code_bucket.bucket
  depends_on = [
    data.archive_file.ingestion_lambda_archive
  ]
  etag = filemd5(data.archive_file.ingestion_lambda_archive.output_path)
}

