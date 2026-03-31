# terraform/main.tf
# This file describes all the AWS infrastructure we need.
# Terraform reads this and creates/updates/destroys resources to match.

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Tell Terraform which AWS region and credentials to use
provider "aws" {
  region = var.aws_region
  # Credentials come from environment variables:
  # AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
}

# ── S3 Bucket (Data Lake) ─────────────────────────────────────────────────────
# This single bucket holds all our data, organized by folder (bronze/, gold/)
resource "aws_s3_bucket" "data_lake" {
  bucket = var.s3_bucket_name

  tags = {
    Project = "ev-pipeline"
    ManagedBy = "terraform"
  }
}

# Block all public access — our data should never be publicly readable
resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Glue Data Catalog (Schema Registry for Athena) ───────────────────────────
# Glue Data Catalog is a free metadata store — it tells Athena where data lives
# and what schema (columns, types) it has. Think of it as a table of contents.
resource "aws_glue_catalog_database" "ev_pipeline" {
  name        = "ev_pipeline"
  description = "EV market intelligence pipeline — dbt gold layer tables"
}

# ── Athena Workgroup ──────────────────────────────────────────────────────────
# A workgroup is a container for Athena query settings.
# We save query results to S3 (required — Athena has nowhere else to put them).
resource "aws_athena_workgroup" "ev_pipeline" {
  name = "ev_pipeline"

  configuration {
    result_configuration {
      # Athena writes query results here
      output_location = "s3://${var.s3_bucket_name}/athena-results/"
    }
  }

  tags = {
    Project = "ev-pipeline"
  }
}
