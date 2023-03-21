#!/bin/bash


SUFFIX=$(date +%s)
aws s3api  create-bucket --bucket nc-terraform-state-${SUFFIX} --region us-east-1

