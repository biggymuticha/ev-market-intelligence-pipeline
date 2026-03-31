# terraform/outputs.tf
# Outputs print useful values after terraform apply completes.

output "s3_bucket_name" {
  description = "Name of the S3 data lake bucket"
  value       = aws_s3_bucket.data_lake.bucket
}

output "athena_workgroup" {
  description = "Athena workgroup name"
  value       = aws_athena_workgroup.ev_pipeline.name
}

output "glue_database" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.ev_pipeline.name
}
