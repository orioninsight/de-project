terraform {
  backend "s3" {
    bucket = "nc-terraform-state-1679924662" #This will need to be updated per sandbox
    key    = "tote-application/terraform.tfstate"
    region = "us-east-1"
  }
}