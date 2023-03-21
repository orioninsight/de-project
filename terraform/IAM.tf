#creates policy document locally for ingestion_lambda to access required S3 resources
data "aws_iam_policy_document" "ingestion_lambda_code_bucket_access" {
  statement {

    actions = ["s3:GetObject"]

    resources = [
      "${aws_s3_bucket.code_bucket.arn}/ingestion_lambda/*"
    ]
  }
}

#creates above policy in IAM
resource "aws_iam_policy" "ingestion_lambda_code_bucket_access" {
  name_prefix = "s3-access-policy-${var.ingestion_lambda_name}-"
  policy      = data.aws_iam_policy_document.ingestion_lambda_code_bucket_access.json
}

#creates policy locally to allow ingestion_lambda lambda to utilise log group
data "aws_iam_policy_document" "ingestion_lambda_cw_document" {
  statement {

    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:${resource.aws_cloudwatch_log_group.ingestion_lambda_log.name}:*"
    ]
  }
}

# creates above policy in IAM
resource "aws_iam_policy" "cw_policy" {
  name_prefix = "cw-policy-${var.ingestion_lambda_name}-"
  policy      = data.aws_iam_policy_document.ingestion_lambda_cw_document.json
}
