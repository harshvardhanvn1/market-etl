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