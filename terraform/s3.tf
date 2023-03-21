resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = var.code_bucket_prefix
}

resource "aws_s3_bucket" "ingestion_zone_bucket" {
  bucket_prefix = var.ingestion_zone_bucket_prefix
}

