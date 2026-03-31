# terraform/variables.tf
# Variables let you reuse values without hardcoding them.

variable "aws_region" {
  description = "AWS region to deploy resources in"
  type        = string
  default     = "us-east-1"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for the data lake — must be globally unique"
  type        = string
  # No default — you must pass this in (see terraform.tfvars)
}