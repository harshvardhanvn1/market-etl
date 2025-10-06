# AWS Region
variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

# Environment name
variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

# Project name
variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "market-data-etl"
}

# S3 bucket name suffix (will use account ID for uniqueness)
locals {
  account_id    = data.aws_caller_identity.current.account_id
  bucket_suffix = local.account_id
  bucket_name   = "${var.project_name}-${var.environment}-${local.bucket_suffix}"
}