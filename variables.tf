variable "project" {
  description = "A name to identify the APP NAME for S3 Key"
  type        = string
}

variable "sync_objects" {
  type        = bool
  default     = false
  description = "Whether to create Glue Registry"
}

variable "region" {
  description = "Default Region"
  type        = string
  default     = "eu-west-2"
}