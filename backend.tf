# Backend
terraform {
  # `backend` blocks do not support variables, so the following are hard-coded here:
  # - S3 bucket name, which is created in modernisation-platform-account/s3.tf
  backend "s3" {
    bucket               = "dpr-terraform-state-development"
    acl                  = "bucket-owner-full-control"   
    encrypt              = true
    key                  = "application/hmpps-digital-prison-reporting-glue-poc/terraform.tfstate"
    region               = "eu-west-2"
  }
}
