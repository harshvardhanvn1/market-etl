# Market Data ETL Pipeline

> Status: M1 Complete - Production pipeline processing 600M+ trades

A production-grade, cloud-native data engineering pipeline for ingesting, transforming, and analyzing market data at scale using AWS serverless technologies.

## Project Overview

This project demonstrates building a complete data lakehouse architecture on AWS that:
- Ingests bulk historical crypto market data (632 million rows)
- Transforms raw data into analytics-ready Parquet format with streaming architecture
- Enables SQL analytics via Athena with partition pruning optimization
- Uses fully automated Infrastructure as Code with Terraform
- Implements production-grade data quality checks and error handling

**Why This Project Matters:**
- Industry Realistic: Mirrors how quant firms, exchanges, and fintech companies manage market data
- Scale: Handles massive datasets with proper partitioning and distributed processing
- Cloud-Native: Fully serverless, no infrastructure management
- Cost-Optimized: Streaming architecture, partition pruning, and efficient storage formats
- Transferable Skills: Patterns apply to trading, risk, fraud detection, IoT, and clickstream analytics

---

## Architecture

**Data Flow:**
```
HTTP Downloads → Glue PyShell (Downloader) 
  → S3 Raw Zone (immutable archives)
  → Glue PyShell (Streaming Unzipper)
  → S3 Raw Unzipped (CSVs)
  → Glue Spark ETL (validate, normalize, Parquet)
  → S3 Processed Zone (partitioned Parquet)
  → Athena (SQL analytics)
```

**S3 Data Lake Zones:**
- `raw/` - Immutable source data (ZIP archives from Binance)
- `raw_unzipped/` - Intermediate CSVs for Spark processing
- `processed/` - Clean, partitioned Parquet files
- `athena-results/` - Query result cache

**Key Design Decisions:**
- Streaming unzipper with S3 multipart uploads (no size limits)
- Separate preprocessing from transformation (Python Shell vs Spark)
- Hive-style partitioning for Athena query optimization
- Production-grade data quality validation (including leap year logic)

---

## Technology Stack

**Cloud Platform:** AWS
- Storage: S3 (data lake with lifecycle policies)
- Catalog: Glue Data Catalog
- ETL: Glue Spark jobs, Glue PyShell scripts
- Analytics: Athena (serverless SQL)
- Orchestration: Terraform for Infrastructure as Code

**Data Sources:**
- Binance public data archives (trades - tick-level execution data)
- 3 symbols: BTCUSDT, ETHUSDT, BNBUSDT
- 3 months: July-September 2025
- Format: Monthly ZIP archives containing CSVs

---

## Current Status - Milestone M1 Complete

### Infrastructure Deployed (13 AWS Resources)

**S3 Data Lake:**
- Bucket: `market-data-etl-dev-<account-id>`
- Versioning enabled
- Server-side encryption (AES256)
- Public access blocked
- Lifecycle policies: Archive raw/ to Glacier after 90 days, delete temp/ after 7 days

**Glue Data Catalog:**
- Database: `market_data`
- Table: `trades_binance` (external table pointing to Parquet files)
- 270+ partitions auto-discovered

**Glue Jobs:**
1. Downloader (Python Shell) - Downloads ZIP files from Binance
2. Unzipper (Python Shell) - Streaming extraction with S3 multipart upload
3. Spark ETL (10 G.1X workers) - CSV to Parquet transformation with DQ checks

**IAM Security:**
- Role: `market-data-etl-dev-glue-job-role`
- Least privilege permissions scoped to data lake bucket only
- CloudWatch logging enabled

All resources tagged for cost allocation and management tracking.

---

## M1 Completion Summary

### Data Pipeline Results

**Volume:**
- Total rows processed: 632,046,300 trades
- BTCUSDT: 213,344,371 trades
- ETHUSDT: 322,638,514 trades  
- BNBUSDT: 96,063,415 trades
- Raw data: ~5 GB compressed (ZIP files)
- Processed data: ~2.5 GB (Parquet with Snappy compression)

**Performance:**
- Downloader: 9 files in ~45 minutes (~$0.33 cost)
- Unzipper: Streaming mode, ~15 minutes (~$0.15 cost)
- Spark ETL: 632M rows in ~18 minutes on G.1X workers (~$2.20 cost)
- Athena queries: 2-3 seconds for aggregations with partition pruning
- Total pipeline cost: ~$2.70 per full run

**Data Quality:**
- Removed ~0.05% of records with invalid dates (Sept 31, Feb 29 in non-leap years, etc.)
- Implemented leap year validation logic
- Timestamp conversion from milliseconds to proper datetime
- Price and quantity validation (non-null, positive values)

**Partitioning Efficiency:**
- Schema: year/month/day/symbol
- Query with partition filters: 67M rows scanned
- Query without partition filters: 213M rows scanned (3x more)
- Cost savings: 70% reduction with proper partition usage

---

## Key Technical Challenges Solved

### Challenge 1: Spark Pattern Matching with ZIP Files
**Problem:** Spark's CSV reader with wildcards couldn't resolve ZIP file paths in Glue 4.0  
**Solution:** Implemented streaming unzipper as separate preprocessing layer

### Challenge 2: Memory Limitations
**Problem:** Workers OOM when trying to load 4-5 GB uncompressed CSVs  
**Solution:** Streaming architecture - unzipper uses S3 multipart upload in 5MB chunks, never exceeds 50MB memory regardless of file size

### Challenge 3: S3 Upload Size Limits  
**Problem:** Single PUT limited to 5GB  
**Solution:** S3 multipart upload API with proper error handling and abort logic

### Challenge 4: Data Quality Issues
**Problem:** Source data contained invalid dates (Sept 31, Feb 29 in non-leap years)  
**Solution:** Comprehensive date validation with proper leap year calculation

### Challenge 5: Partition Column Extraction
**Problem:** String manipulation on dates created NULL values  
**Solution:** Used Spark's built-in dayofmonth() function instead of substr() operations

---

## Repository Structure

```
market-etl/
├── README.md
├── .gitignore
├── infra/
│   └── terraform/
│       ├── providers.tf
│       ├── variables.tf
│       ├── outputs.tf
│       └── main.tf (S3, Glue, IAM, Catalog Table)
├── jobs/
│   ├── downloader/
│   │   ├── glue_binance_downloader.py
│   │   └── glue_binance_unzipper.py
│   └── etl/
│       └── glue_spark_trades_etl.py
└── sql/
    ├── athena_create_trades_table.sql
    ├── athena_repair_partitions.sql
    └── athena_sample_queries.sql
```

---

## Setup Instructions

### Prerequisites

- AWS Account with billing enabled
- Terraform >= 1.0
- AWS CLI v2 configured with credentials
- Git for version control
- Python 3.9+ (for local testing/development)

### Deployment

**1. Clone Repository**
```bash
git clone https://github.com/YOUR-USERNAME/market-etl.git
cd market-etl
```

**2. Configure AWS Credentials**
```bash
aws configure
# Enter your AWS Access Key ID and Secret Access Key
# Region: us-east-1
# Output format: json
```

**3. Deploy Infrastructure**
```bash
cd infra/terraform
terraform init
terraform plan
terraform apply
```

**4. Run Pipeline**
```bash
# Step 1: Download data
aws glue start-job-run --job-name market-data-etl-dev-binance-downloader

# Step 2: Unzip files (after downloader completes)
aws glue start-job-run --job-name market-data-etl-dev-binance-unzipper

# Step 3: Transform to Parquet (after unzipper completes)
aws glue start-job-run --job-name market-data-etl-dev-spark-trades-etl
```

**5. Query Data in Athena**
```bash
# Open AWS Athena Console
# Run: MSCK REPAIR TABLE market_data.trades_binance;
# Start querying with SQL
```

---

## Cost Breakdown

**Current Scale (M1):**
- S3 storage (5GB): ~$0.12/month
- Glue downloader: ~$0.33/run
- Glue unzipper: ~$0.15/run
- Glue Spark ETL: ~$2.20/run
- Athena queries (10GB scanned): ~$0.05
- **Total: ~$2.85 per pipeline run + $0.12/month storage**

**Projected at Production Scale (M2):**
- 50 symbols × 3-5 years: ~$15-20/month

**Cost Optimization Strategies:**
- Lifecycle policies auto-archive to Glacier
- Partition pruning minimizes Athena scans (70% cost reduction demonstrated)
- Streaming architecture avoids compute over-provisioning
- Parquet compression reduces storage and scan costs by 10x vs CSV

---

## Sample Queries

**Basic Row Count by Symbol:**
```sql
SELECT symbol, COUNT(*) as trades
FROM market_data.trades_binance
GROUP BY symbol;
```

**Minute-by-Minute VWAP (Volume-Weighted Average Price):**
```sql
SELECT 
    date_trunc('minute', trade_time) as minute,
    SUM(price * quantity) / NULLIF(SUM(quantity), 0) as vwap
FROM market_data.trades_binance
WHERE year = 2025 AND month = 7 AND day = 15
  AND symbol = 'BTCUSDT'
GROUP BY date_trunc('minute', trade_time)
ORDER BY minute;
```

**Order Flow Analysis:**
```sql
SELECT 
    symbol,
    CASE WHEN is_buyer_maker THEN 'Sell' ELSE 'Buy' END as order_type,
    COUNT(*) as trades,
    SUM(quote_qty) as total_volume_usd
FROM market_data.trades_binance
WHERE year = 2025 AND month = 7
GROUP BY symbol, is_buyer_maker;
```

More queries available in `sql/athena_sample_queries.sql`

---

## Project Roadmap

### M0: Bootstrap & Infrastructure (Complete)
- [x] AWS account and IAM setup
- [x] S3 data lake with security and lifecycle policies
- [x] Glue Data Catalog database
- [x] IAM roles with least privilege
- [x] Terraform IaC with provider locking
- [x] Git repository with .gitignore

### M1: Dev-Scale Pipeline (Complete)
- [x] Glue PyShell downloader (9 files, ~5GB)
- [x] Streaming unzipper with S3 multipart upload
- [x] Glue Spark ETL with production-grade DQ checks
- [x] Partition by year/month/day/symbol
- [x] Athena external table with Terraform
- [x] Query performance validation
- [x] Partition pruning demonstration (3x efficiency gain)
- [x] Cost and performance metrics documented

### M2: Scale Out (Planned)
- [ ] Expand to 50 symbols × 3-5 years
- [ ] Advanced DQ checks and monitoring
- [ ] Performance tuning for larger datasets
- [ ] Comprehensive cost analysis

### M3: Warehouse Benchmark (Optional)
- [ ] Load to Redshift Serverless
- [ ] Query performance comparison

### M4: Real-Time Replay (Optional)
- [ ] Fargate producer to Kinesis
- [ ] Stream processing demonstration

### M5: Polish & Documentation (Planned)
- [ ] Architecture diagram
- [ ] Comprehensive documentation
- [ ] Interview talking points

---

## Key Metrics (M1)

**Volume:**
- Total rows: 632,046,300
- Files processed: 9 ZIP archives
- Partitions created: 270+
- Data compression: 50% (5GB raw → 2.5GB Parquet)

**Performance:**
- Pipeline throughput: ~350M rows/hour
- Athena query latency: 2-3 seconds (with partition filters)
- Partition pruning efficiency: 70% cost reduction

**Cost:**
- Cost per million rows: ~$0.004
- Storage cost: $0.024/GB/month
- Query cost: $0.005/GB scanned

---

## Learning Outcomes

**Demonstrated Skills:**
- Cloud-native data engineering on AWS
- Infrastructure as Code with Terraform
- S3 data lake design (zones, partitioning, lifecycle)
- Serverless ETL with AWS Glue
- Production-grade error handling and DQ checks
- IAM security and least privilege
- SQL analytics with Athena
- Cost optimization strategies
- Streaming architecture for unlimited scale
- Performance tuning and partition optimization

---

## Security & Best Practices

- All S3 buckets have public access blocked
- Server-side encryption enabled (AES256)
- IAM roles use least privilege permissions
- Credentials never committed to Git
- Resources tagged for cost tracking
- Versioning enabled for data protection
- Infrastructure managed via Terraform (reproducible, auditable)
- Idempotent jobs (safe to re-run)

---

## License

This project is for educational and portfolio purposes.

---

## Status

Active Development - M1 Complete, Ready for Scale Out (M2)