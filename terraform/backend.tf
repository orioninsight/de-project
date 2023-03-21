terraform {
    backend "s3" {
        bucket = "nc-terraform-state-1679397451"
        key = "tote-application/terraform.tfstate"
        region = "us-east-1"
    }
}