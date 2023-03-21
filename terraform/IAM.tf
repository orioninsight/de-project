#creates policy document locally
data "aws_iam_policy_document" "ingestion_lambda_code_bucket_access" {
  statement {

    actions = ["s3:GetObject"]

    resources = [
      "${aws_s3_bucket.code_bucket.arn}/*",
      "${aws_s3_bucket.data_bucket.arn}/*",
    ]
  }
}

#creates above policy in IAM
resource "aws_iam_policy" "ingestion_lambda_code_bucket_access" {
  name_prefix = "s3-access-policy-${var.ingestion_lambda_name}"
  policy      = data.aws_iam_policy_document.ingestion_lambda_code_bucket_access.json
}

