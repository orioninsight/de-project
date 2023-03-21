variable "code_bucket_prefix" {
    type = string
    default = "lambda-code-"
}

variable "ingestion_zone_bucket_prefix" {
    type = string
    default = "ingestion-zone-"
}

variable "extraction_lambda_name" {
    type = string
    default = "extraction-lambda-"
}