variable "region" {
  default = "eu-west-2"
}

provider "aws" {
  region = "${var.region}"
}

resource "aws_s3_bucket_object" "object1" {
    count       = var.sync_objects ? 1 : 0

    for_each    = fileset("src/", "*.py")
    bucket      = arn:aws:s3:::dpr-glue-jobs-development-20220916083016134900000005
    key         = "${var.project}/${each.value}"
    source      = "src/${each.value}"
    etag        = filemd5("src/${each.value}")
}