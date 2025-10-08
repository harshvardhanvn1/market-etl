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

#######################################
# S3 Bucket for Glue Scripts
#######################################

# Upload the downloader script to S3
resource "aws_s3_object" "glue_downloader_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/glue_binance_downloader.py"
  source = "../../jobs/downloader/glue_binance_downloader.py"
  etag   = filemd5("../../jobs/downloader/glue_binance_downloader.py")

  # Encrypt the script
  server_side_encryption = "AES256"

  tags = {
    Name        = "Glue Downloader Script"
    Description = "Python script for downloading Binance data"
  }
}

#######################################
# AWS Glue Job - Binance Downloader
#######################################

resource "aws_glue_job" "binance_downloader" {
  name     = "${var.project_name}-${var.environment}-binance-downloader"
  role_arn = aws_iam_role.glue_job_role.arn

  # Python Shell job type (not Spark)
  command {
    name            = "pythonshell"
    script_location = "s3://${aws_s3_bucket.data_lake.id}/${aws_s3_object.glue_downloader_script.key}"
    python_version  = "3.9"
  }

  # Default job parameters (can be overridden when running)
  default_arguments = {
    "--job-language"        = "python"
    "--job-bookmark-option" = "job-bookmark-disable"

    # Job name (required by our script)
    "--JOB_NAME" = "${var.project_name}-${var.environment}-binance-downloader"

    # Our custom parameters
    "--BUCKET_NAME"      = aws_s3_bucket.data_lake.id
    "--SYMBOLS"          = "BTCUSDT,ETHUSDT,BNBUSDT"
    "--START_YEAR_MONTH" = "2025-07"
    "--END_YEAR_MONTH"   = "2025-09"
    "--DATA_TYPE"        = "trades"

    # Enable CloudWatch metrics
    "--enable-metrics"                   = ""
    "--enable-continuous-cloudwatch-log" = "true"
  }

  # Python Shell specific settings
  max_capacity = 1 # 1 DPU for Python Shell

  # Retry and timeout settings
  max_retries = 1
  timeout     = 120 # 2 hours (in minutes)

  # CloudWatch log group
  glue_version = "3.0"

  tags = {
    Name        = "Binance Data Downloader"
    Description = "Downloads historical market data from Binance to S3"
  }

  # Ensure script is uploaded before creating job
  depends_on = [aws_s3_object.glue_downloader_script]
}

#######################################
# S3 Object - Unzipper Script
#######################################

resource "aws_s3_object" "glue_unzipper_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/glue_binance_unzipper.py"
  source = "../../jobs/downloader/glue_binance_unzipper.py"
  etag   = filemd5("../../jobs/downloader/glue_binance_unzipper.py")

  server_side_encryption = "AES256"

  tags = {
    Name        = "Glue Unzipper Script"
    Description = "Streaming unzipper using S3 multipart upload"
  }
}

#######################################
# Glue Job - Unzipper
#######################################

resource "aws_glue_job" "binance_unzipper" {
  name     = "${var.project_name}-${var.environment}-binance-unzipper"
  role_arn = aws_iam_role.glue_job_role.arn

  command {
    name            = "pythonshell"
    script_location = "s3://${aws_s3_bucket.data_lake.id}/${aws_s3_object.glue_unzipper_script.key}"
    python_version  = "3.9"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--JOB_NAME"                         = "${var.project_name}-${var.environment}-binance-unzipper"
    "--BUCKET_NAME"                      = aws_s3_bucket.data_lake.id
    "--DATA_TYPE"                        = "trades"
    "--enable-metrics"                   = ""
    "--enable-continuous-cloudwatch-log" = "true"
  }

  max_capacity = 1
  max_retries  = 1
  timeout      = 180 # 3 hours for large files
  glue_version = "3.0"

  tags = {
    Name        = "Binance Unzipper"
    Description = "Streaming unzipper for production-grade file processing"
  }

  depends_on = [aws_s3_object.glue_unzipper_script]
}

#######################################
# S3 Object - Spark ETL Script
#######################################

resource "aws_s3_object" "glue_etl_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/glue_spark_trades_etl.py"
  source = "../../jobs/etl/glue_spark_trades_etl.py"
  etag   = filemd5("../../jobs/etl/glue_spark_trades_etl.py")

  server_side_encryption = "AES256"

  tags = {
    Name = "Glue Spark ETL Script"
  }
}

#######################################
# AWS Glue Job - Spark ETL
#######################################

resource "aws_glue_job" "spark_trades_etl" {
  name     = "${var.project_name}-${var.environment}-spark-trades-etl"
  role_arn = aws_iam_role.glue_job_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_lake.id}/${aws_s3_object.glue_etl_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--JOB_NAME"                         = "${var.project_name}-${var.environment}-spark-trades-etl"
    "--BUCKET_NAME"                      = aws_s3_bucket.data_lake.id
    "--enable-metrics"                   = ""
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.data_lake.id}/logs/spark/"
  }

  # Spark job settings
  glue_version = "4.0"

  # 10 workers (enough for 9 files + shuffling)
  number_of_workers = 10
  worker_type       = "G.1X"

  max_retries = 1
  timeout     = 180 # 3 hours

  tags = {
    Name = "Spark Trades ETL"
  }

  depends_on = [aws_s3_object.glue_etl_script]
}