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

#ingestion zone lambda name without suffix 
variable "ingestion_lambda_name" {
  type    = string
  default = "extract_db_demo"
}

#lambda handler for ingestion zone lambda
variable "ingestion_lambda_handler" {
  type    = string
  default = "extract_db_demo.extract_db_handler"
}