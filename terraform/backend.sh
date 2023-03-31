#!/bin/bash

BUCKET_PREFIX=nc-terraform-state-
unset TERRAFORM_S3_BUCKET_NAME
for bucket in `aws s3 ls`; do
    if [[ "$bucket" =~ ^"$BUCKET_PREFIX" ]]; then
        echo "Using existing S3 bucket for Terraform state: $bucket"
        TERRAFORM_S3_BUCKET_NAME="$bucket"
        break
    fi
done

if [[ -z "$TERRAFORM_S3_BUCKET_NAME" ]]; then
    echo "Creating new S3 bucket for Terraform state..."
    SUFFIX=$(date +%s)
    TERRAFORM_S3_BUCKET_NAME=${BUCKET_PREFIX}${SUFFIX}
    aws s3api  create-bucket --bucket ${TERRAFORM_S3_BUCKET_NAME} --region us-east-1
fi

eval "cat <<EOF
$(<backend.tf.template)
EOF
" > backend.tf 2> /dev/null
