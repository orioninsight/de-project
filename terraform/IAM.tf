#creates policy document locally for ingestion_lambda to access required S3 resources
data "aws_iam_policy_document" "ingestion_lambda_bucket_access" {
  statement {

    actions = ["s3:GetObject"]

    resources = [
      "${aws_s3_bucket.code_bucket.arn}/extraction_lambda/*"
    ]
  }
  statement {

    actions = ["s3:PutObject", "s3:ListBucket"]

    resources = [
      "${aws_s3_bucket.ingestion_zone_bucket.arn}/*"
    ]
  }
}

#creates above policy in IAM
resource "aws_iam_policy" "ingestion_lambda_bucket_access" {
  name_prefix = "s3-access-policy-extraction-lambda-"
  policy      = data.aws_iam_policy_document.ingestion_lambda_bucket_access.json
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

#creates policy locally to allow ingestion_lambda lambda to access specific secret in secrets manager 
data "aws_iam_policy_document" "ingestion_lambda_secretsmanager_document" {
  statement {

    actions = ["secretsmanager:GetSecretValue"]

    resources = [
      "${data.aws_secretsmanager_secret.database_secret.arn}"
    ]
  }
}

# creates above policy in IAM
resource "aws_iam_policy" "ingestion_lamba_cw_policy" {
  name_prefix = "cw-policy-extraction-lambda-"
  policy      = data.aws_iam_policy_document.ingestion_lambda_cw_document.json
}

# creates policy for lambda to access secretsmanager in IAM
resource "aws_iam_policy" "ingestion_lamba_secretsmanager_policy" {
  name_prefix = "secretsmanager-policy-extraction-lambda-"
  policy      = data.aws_iam_policy_document.ingestion_lambda_secretsmanager_document.json
}

#creates policy locally to allow ingestion_lambda lambda to call transform_lambda
data "aws_iam_policy_document" "ingestion_lambda_call_transform_document" {
  statement {

    actions = ["lambda:InvokeFunction"]

    resources = [
      "${aws_lambda_function.transform_lambda.arn}/*"
    ]
  }
}

# creates above policy in IAM
resource "aws_iam_policy" "ingestion_lamba_call_transform_policy" {
  name_prefix = "call-transform-policy-extraction-lambda-"
  policy      = data.aws_iam_policy_document.ingestion_lambda_call_transform_document.json
}

# creates lambda role 
resource "aws_iam_role" "ingestion_lambda_role" {
  name_prefix        = "role-extraction-lambda-"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

# attach IAM policy to lambda role 
resource "aws_iam_role_policy_attachment" "ingestion_lambda_s3_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.ingestion_lambda_bucket_access.arn
}

resource "aws_iam_role_policy_attachment" "ingestion_lambda_cw_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.ingestion_lamba_cw_policy.arn
}

resource "aws_iam_role_policy_attachment" "ingestion_lambda_secretsmanager_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.ingestion_lamba_secretsmanager_policy.arn
}

resource "aws_iam_role_policy_attachment" "ingestion_lambda_call_transform_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.ingestion_lamba_call_transform_policy.arn
}
### trasformation_lambda ###

#creates policy document locally for transform_lambda to access required S3 resources
data "aws_iam_policy_document" "transform_lambda_bucket_access" {
  statement {

    actions = ["s3:GetObject"]

    resources = [
      "${aws_s3_bucket.code_bucket.arn}/transform_lambda/*"
    ]
  }
  statement {

    actions = ["s3:GetObject", "s3:ListBucket"]

    resources = [
      "${aws_s3_bucket.ingestion_zone_bucket.arn}/*"
    ]
  }
  statement {

    actions = ["s3:PutObject", "s3:ListBucket"]

    resources = [
      "${aws_s3_bucket.transformed_zone_bucket.arn}/*"
    ]
  }
}

#creates above policy in IAM
resource "aws_iam_policy" "transform_lambda_bucket_access" {
  name_prefix = "s3-access-policy-transform-lambda-"
  policy      = data.aws_iam_policy_document.transform_lambda_bucket_access.json
}

#creates policy locally to allow transform_lambda lambda to utilise log group
data "aws_iam_policy_document" "transform_lambda_cw_document" {
  statement {

    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:${resource.aws_cloudwatch_log_group.transform_lambda_log.name}:*"
    ]
  }
}

# creates above policy in IAM
resource "aws_iam_policy" "transform_lamba_cw_policy" {
  name_prefix = "cw-policy-transform-lambda-"
  policy      = data.aws_iam_policy_document.transform_lambda_cw_document.json
}



# creates transform-lambda role 
resource "aws_iam_role" "transform_lambda_role" {
  name_prefix        = "role-transform-lambda-"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

# attach IAM policy to transform-lambda role 
resource "aws_iam_role_policy_attachment" "transform_lambda_s3_policy_attachment" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.transform_lambda_bucket_access.arn
}

resource "aws_iam_role_policy_attachment" "transform_lambda_cw_policy_attachment" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.transform_lamba_cw_policy.arn
}