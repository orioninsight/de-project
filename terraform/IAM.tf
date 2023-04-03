#############################
### extraction lambda IAM ###
#############################

# create policy document locally for extraction_lambda to access required S3 resources
data "aws_iam_policy_document" "extraction_lambda_bucket_access" {
  # get code from code bucket
  statement {

    actions = ["s3:GetObject"]

    resources = [
      "${aws_s3_bucket.code_bucket.arn}/extraction_lambda/*"
    ]
  }

  # put, get objects, and list bucket contents in extraction zone bucket
  statement {
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:ListBucket"
    ]
    resources = [
      "${aws_s3_bucket.ingestion_zone_bucket.arn}",
      "${aws_s3_bucket.ingestion_zone_bucket.arn}/*"
    ]
  }
}

# create S3 policy in IAM
resource "aws_iam_policy" "extraction_lambda_bucket_access" {
  name_prefix = "extraction-lambda-s3-access-policy-"
  policy      = data.aws_iam_policy_document.extraction_lambda_bucket_access.json
}


#create policy locally to allow extraction_lambda lambda to utilise log group
data "aws_iam_policy_document" "extraction_lambda_cw_document" {
  # allow create log stream and log events in the group
  statement {

    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:${resource.aws_cloudwatch_log_group.extraction_lambda_log.name}:*"
    ]
  }
}

# creates cw policy in IAM
resource "aws_iam_policy" "extraction_lamba_cw_policy" {
  name_prefix = "extraction-lambda-cw-policy-"
  policy      = data.aws_iam_policy_document.extraction_lambda_cw_document.json
}

#creates policy locally to allow extraction_lambda lambda to access specific database secret from secrets manager 
data "aws_iam_policy_document" "extraction_lambda_secretsmanager_document" {
  statement {

    actions = ["secretsmanager:GetSecretValue"]

    resources = [
      "${data.aws_secretsmanager_secret.database_secret.arn}"
    ]
  }
}

# creates policy for extraction lambda to access db secret secretsmanager in IAM
resource "aws_iam_policy" "extraction_lambda_secretsmanager_policy" {
  name_prefix = "extraction-lambda-secretsmanager-policy-"
  policy      = data.aws_iam_policy_document.extraction_lambda_secretsmanager_document.json
}

#creates policy locally to allow extraction_lambda to invoke transform_lambda
data "aws_iam_policy_document" "extraction_lambda_invoke_transform_document" {
  statement {

    actions = ["lambda:InvokeFunction"]

    resources = [
      aws_lambda_function.transform_lambda.arn
    ]
  }
}

# creates invoke policy in IAM
resource "aws_iam_policy" "extraction_lambda_invoke_transform_policy" {
  name_prefix = "extraction-lambda-invoke-transform-policy-"
  policy      = data.aws_iam_policy_document.extraction_lambda_invoke_transform_document.json
}

# creates lambda role for extraction lambda
resource "aws_iam_role" "extraction_lambda_role" {
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

# attach extraction S3 policy to extration lambda role
resource "aws_iam_role_policy_attachment" "extraction_lambda_s3_policy_attachment" {
  role       = aws_iam_role.extraction_lambda_role.name
  policy_arn = aws_iam_policy.extraction_lambda_bucket_access.arn
}

# attach extraction cw policy to extration lambda role
resource "aws_iam_role_policy_attachment" "extraction_lambda_cw_policy_attachment" {
  role       = aws_iam_role.extraction_lambda_role.name
  policy_arn = aws_iam_policy.extraction_lamba_cw_policy.arn
}

# attach extraction secret manager policy to extration lambda role
resource "aws_iam_role_policy_attachment" "extraction_lambda_secretsmanager_policy_attachment" {
  role       = aws_iam_role.extraction_lambda_role.name
  policy_arn = aws_iam_policy.extraction_lambda_secretsmanager_policy.arn
}

# attach extraction invoke policy to extration lambda role
resource "aws_iam_role_policy_attachment" "extraction_lambda_invoke_transform_policy_attachment" {
  role       = aws_iam_role.extraction_lambda_role.name
  policy_arn = aws_iam_policy.extraction_lambda_invoke_transform_policy.arn
}

############################
### trasformation_lambda ###
############################

# creates policy document locally for transform_lambda to access required S3 resources
data "aws_iam_policy_document" "transform_lambda_bucket_access" {
  # get objects from code bucket
  statement {

    actions = ["s3:GetObject"]

    resources = [
      "${aws_s3_bucket.code_bucket.arn}/transform_lambda/*"
    ]
  }
  statement {
    # get and list objects from extraction zone bucket
    actions = ["s3:GetObject", "s3:ListBucket"]

    resources = [
      "${aws_s3_bucket.ingestion_zone_bucket.arn}/*",
      "${aws_s3_bucket.ingestion_zone_bucket.arn}"
    ]
  }
  statement {

    actions = ["s3:PutObject", "s3:ListBucket"]

    resources = [
      "${aws_s3_bucket.transformed_zone_bucket.arn}/*",
      "${aws_s3_bucket.transformed_zone_bucket.arn}"
    ]
  }
}

#creates transform s3 policy in IAM
resource "aws_iam_policy" "transform_lambda_bucket_access" {
  name_prefix = "transform-lambda-s3-access-policy-"
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

# creates cw policy in IAM
resource "aws_iam_policy" "transform_lamba_cw_policy" {
  name_prefix = "transform-lambda-cw-policy-"
  policy      = data.aws_iam_policy_document.transform_lambda_cw_document.json
}

#creates policy locally to allow trasnform_lambda to invoke load_lambda
data "aws_iam_policy_document" "transform_lambda_invoke_load_document" {
  statement {

    actions = ["lambda:InvokeFunction"]

    resources = [
      aws_lambda_function.load_lambda.arn
    ]
  }
}

# creates invoke policy in IAM
resource "aws_iam_policy" "transform_lambda_invoke_load_policy" {
  name_prefix = "transform-lambda-invoke-load-policy-"
  policy      = data.aws_iam_policy_document.transform_lambda_invoke_load_document.json
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

# attach s3 policy to transform-lambda role 
resource "aws_iam_role_policy_attachment" "transform_lambda_s3_policy_attachment" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.transform_lambda_bucket_access.arn
}

# attach cw policy to transform-lambda role
resource "aws_iam_role_policy_attachment" "transform_lambda_cw_policy_attachment" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.transform_lamba_cw_policy.arn
}

# attach transform invoke policy to transform lambda role
resource "aws_iam_role_policy_attachment" "transform_lambda_invoke_load_policy_attachment" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.transform_lambda_invoke_load_policy.arn
}

###################
### load lambda ###
###################

# creates policy document locally for load_lambda to access required S3 resources
data "aws_iam_policy_document" "load_lambda_bucket_access" {
  # get objects from code bucket
  statement {

    actions = ["s3:GetObject"]

    resources = [
      "${aws_s3_bucket.code_bucket.arn}/load_lambda/*"
    ]
  }
  statement {
    # get and list objects from extraction zone bucket
    actions = ["s3:GetObject", "s3:ListBucket"]

    resources = [
      "${aws_s3_bucket.transformed_zone_bucket.arn}/*"
    ]
  }
}

#creates above policy in IAM
resource "aws_iam_policy" "load_lambda_bucket_access" {
  name_prefix = "load-lambda-s3-access-policy-"
  policy      = data.aws_iam_policy_document.load_lambda_bucket_access.json
}

#creates policy locally to allow load_lambda lambda to utilise log group
data "aws_iam_policy_document" "load_lambda_cw_document" {
  statement {

    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:${resource.aws_cloudwatch_log_group.load_lambda_log.name}:*"
    ]
  }
}

# creates cw policy in IAM
resource "aws_iam_policy" "load_lamba_cw_policy" {
  name_prefix = "load-lambda-cw-policy-"
  policy      = data.aws_iam_policy_document.load_lambda_cw_document.json
}

# creates load-lambda role 
resource "aws_iam_role" "load_lambda_role" {
  name_prefix        = "role-load-lambda-"
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

# attach s3 policy to load-lambda role 
resource "aws_iam_role_policy_attachment" "load_lambda_s3_policy_attachment" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.load_lambda_bucket_access.arn
}

# attach cw policy to load-lambda role
resource "aws_iam_role_policy_attachment" "load_lambda_cw_policy_attachment" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.load_lamba_cw_policy.arn
}
