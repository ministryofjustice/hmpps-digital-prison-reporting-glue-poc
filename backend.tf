# Backend
terraform {
  # `backend` blocks do not support variables, so the following are hard-coded here:
  # - S3 bucket name, which is created in modernisation-platform-account/s3.tf
  backend "s3" {
    bucket               = "digital-prison-reporting-terraform-state"
    acl     = "bucket-owner-full-control"   
    encrypt              = true
    key                  = "terraform.tfstate"
    region               = "eu-west-2"
    workspace_key_prefix = "application/hmpps-digital-prison-reporting-glue-poc" 
  }
}
