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

#trasformed bucket prefix
variable "transformed_bucket_prefix" {
  type    = string
  default = "transformed-zone"
}

#lambda handler for ingestion zone lambda
variable "extraction_lambda_handler" {
  type    = string
  default = "extract_db.extract_db_handler"
}

# email for ingestion zone lambda alerting
variable "email" {
  type    = string
  default = "orioninsight23@gmail.com"
}
