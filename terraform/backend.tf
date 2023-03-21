terraform {
    backend "s3" {
        bucket = "nc-terraform-state-1679394142"
        key = "s3_file_reader/terraform.tfstate"
        region = "us-east-1"
    }
}