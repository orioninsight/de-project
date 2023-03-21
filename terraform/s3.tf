resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = var.code_bucket_prefix
}

