#code bucket prefix
variable "code_bucket_prefix" {
  type    = string
  default = "lambda-code-"
}

#ingestion zone bucket prexfix
variable "ingestion_zone_bucket_prefix" {
  type    = string
  default = "ingestion-zone-"
}

#lambda handler for ingestion zone lambda
variable "ingestion_lambda_handler" {
  type    = string
  default = "handler.example_handler"
}

# email for ingestion zone lambda alerting
variable "email" {
  type    = string
  default = "orioninsight23@gmail.com"
}
