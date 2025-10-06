#######################################
# S3 Data Lake Bucket
#######################################

# Main bucket
resource "aws_s3_bucket" "data_lake" {
  bucket = local.bucket_name

  tags = {
    Name        = "Market Data Lake"
    Description = "Data lake for market data ETL pipeline"
  }
}

# Block all public access (security best practice)
resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enable versioning (data protection)
resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Server-side encryption (security requirement)
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Lifecycle policy - archive raw data to Glacier after 90 days (cost optimization)
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "archive-raw-to-glacier"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }

  rule {
    id     = "expire-temp-files"
    status = "Enabled"

    filter {
      prefix = "temp/"
    }

    expiration {
      days = 7
    }
  }
}

#######################################
# Glue Data Catalog Database
#######################################

resource "aws_glue_catalog_database" "market_data" {
  name        = "market_data"
  description = "Data catalog for market data ETL pipeline - stores metadata for trades, klines, and other market data tables"

  # Default location for tables in this database
  location_uri = "s3://${aws_s3_bucket.data_lake.id}/processed/"

  tags = {
    Name        = "Market Data Catalog"
    Description = "Glue catalog database for Athena queries and ETL jobs"
  }
}

#######################################
# IAM Role for Glue Jobs
#######################################

# IAM Role for Glue ETL Jobs
resource "aws_iam_role" "glue_job_role" {
  name        = "${var.project_name}-${var.environment}-glue-job-role"
  description = "IAM role for Glue ETL jobs to access S3 and Glue Catalog"

  # Trust policy - allows Glue service to assume this role
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name        = "Glue Job Role"
    Description = "Service role for Glue ETL jobs"
  }
}

# Attach AWS-managed Glue service policy
resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Custom inline policy for S3 data lake access
resource "aws_iam_role_policy" "glue_s3_access" {
  name = "s3-data-lake-access"
  role = aws_iam_role.glue_job_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn
        ]
      }
    ]
  })
}

# CloudWatch Logs permissions for Glue job logging
resource "aws_iam_role_policy" "glue_cloudwatch_logs" {
  name = "cloudwatch-logs-access"
  role = aws_iam_role.glue_job_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*"
      }
    ]
  })
}