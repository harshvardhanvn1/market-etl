# AWS Account Information
output "account_id" {
  description = "AWS Account ID"
  value       = data.aws_caller_identity.current.account_id
}

output "aws_region" {
  description = "AWS Region in use"
  value       = data.aws_region.current.name
}

# S3 Bucket Information
output "data_lake_bucket_name" {
  description = "Name of the data lake S3 bucket"
  value       = aws_s3_bucket.data_lake.id
}

output "data_lake_bucket_arn" {
  description = "ARN of the data lake S3 bucket"
  value       = aws_s3_bucket.data_lake.arn
}

# Glue Database
output "glue_database_name" {
  description = "Name of the Glue Data Catalog database"
  value       = aws_glue_catalog_database.market_data.name
}

# IAM Role for Glue Jobs
output "glue_job_role_arn" {
  description = "ARN of the IAM role for Glue jobs"
  value       = aws_iam_role.glue_job_role.arn
}