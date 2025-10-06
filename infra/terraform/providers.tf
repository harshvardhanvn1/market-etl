# Terraform version and required providers
terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# AWS Provider configuration
provider "aws" {
  region = var.aws_region

  # Auto-tag all resources
  default_tags {
    tags = {
      Project     = "MarketDataETL"
      ManagedBy   = "Terraform"
      Environment = var.environment
      Owner       = "terraform-admin"
    }
  }
}

# Get current AWS account info
data "aws_caller_identity" "current" {}

# Get available AWS region info
data "aws_region" "current" {}