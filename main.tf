provider "aws" {
  region = var.region
}

resource "aws_s3_bucket_object" "sample_pycode" {
  for_each = fileset("src/", "*.py")
  bucket   = "dpr-glue-jobs-development-20220916083016134900000005"
  key      = "${var.project}/${each.value}"
  source   = "src/${each.value}"
  etag     = filemd5("src/${each.value}")
}