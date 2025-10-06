# Market Data ETL Pipeline

> **Status:** Active Development - M0 Complete, M1 In Progress

A production-grade, cloud-native data engineering pipeline for ingesting, transforming, and analyzing market data at scale using AWS serverless technologies.

## Project Overview

This project demonstrates building a complete data lakehouse architecture on AWS that:
- Ingests bulk historical crypto/forex market data (hundreds of millions to billions of rows)
- Transforms raw data into analytics-ready Parquet format
- Enables SQL analytics via Athena and Redshift
- Simulates real-time data ingestion via Kinesis replay
- Orchestrates workflows with Apache Airflow (MWAA)

**Why This Project Matters:**
- **Industry Realistic:** Mirrors how quant firms, exchanges, and fintech companies manage market data
- **Scale:** Handles massive datasets with proper partitioning and distributed processing
- **Cloud-Native:** Fully serverless, no infrastructure management
- **Cost-Optimized:** Lifecycle policies, partition pruning, and efficient storage formats
- **Transferable Skills:** Patterns apply to trading, risk, fraud detection, IoT, and clickstream analytics

---

## Architecture

**Data Flow:**
```
HTTP Downloads → Glue PyShell (Downloader) 
  → S3 Raw Zone (immutable archives)
  → Glue Spark ETL (validate, normalize, Parquet)
  → S3 Processed Zone (partitioned Parquet)
  → Athena/Redshift (SQL analytics)

Optional: S3 → Fargate → Kinesis → Stream consumers
```

**S3 Data Lake Zones:**
- `raw/` - Immutable source data (CSV/JSON archives)
- `processed/` - Clean, partitioned Parquet files
- `stream/` - Real-time replay sink (optional)

---

## Technology Stack

**Cloud Platform:** AWS
- **Storage:** S3 (data lake)
- **Catalog:** Glue Data Catalog
- **ETL:** Glue Spark jobs, Glue PyShell scripts
- **Analytics:** Athena (serverless SQL), Redshift Serverless (optional)
- **Streaming:** Kinesis Data Streams, Fargate (optional)
- **Orchestration:** Apache Airflow (MWAA)

**Infrastructure:**
- **IaC:** Terraform (AWS Provider v5.0)
- **Version Control:** Git + GitHub (branch protection, PR workflow)

**Data Sources:**
- Binance public data archives (trades, klines/OHLCV)
- HistData forex tick/minute data (optional expansion)

---

## Current Status - Milestone M0 Complete

### **Infrastructure Deployed (10 AWS Resources):**

**S3 Data Lake:**
- Bucket: `market-data-etl-dev-<account-id>`
- Versioning enabled (data protection)
- Server-side encryption (AES256)
- Public access blocked (all 4 settings)
- Lifecycle policies:
  - Archive `raw/` to Glacier after 90 days (cost optimization)
  - Delete `temp/` files after 7 days (cleanup)

**Glue Data Catalog:**
- Database: `market_data`
- Location: Points to S3 processed zone

**IAM Security:**
- IAM Role: `market-data-etl-dev-glue-job-role`
- Trust policy: Glue service can assume role
- Permissions:
  - AWS managed: `AWSGlueServiceRole` (Glue Catalog + CloudWatch)
  - Custom S3 policy: Scoped to data lake bucket only (least privilege)
  - CloudWatch Logs: For Glue job debugging

**All resources tagged for:**
- Cost allocation (`Project`, `Environment`)
- Management tracking (`ManagedBy: Terraform`)
- Ownership (`Owner: terraform-admin`)

---

## Prerequisites

- **AWS Account** with billing enabled
- **Terraform** >= 1.0
- **AWS CLI** v2 configured with credentials
- **Git** for version control
- **Python 3.9+** (for Glue jobs in M1+)

---

## Repository Structure

```
market-etl/
├── README.md                    # This file
├── .gitignore                   # Protects credentials and state files
├── infra/
│   └── terraform/               # Infrastructure as Code
│       ├── providers.tf         # AWS provider configuration
│       ├── variables.tf         # Input variables and locals
│       ├── outputs.tf           # Output values
│       ├── main.tf              # S3, Glue, IAM resources
│       └── .terraform.lock.hcl  # Provider version lock
├── jobs/
│   ├── downloader/              # Glue PyShell scripts (M1)
│   └── etl/                     # Glue Spark ETL jobs (M1)
├── airflow/
│   └── dags/                    # Airflow DAGs (M1)
├── sql/                         # Athena DDL and queries (M1)
└── ops/                         # Operations docs (cost, teardown)
```

---

## Setup Instructions

### **1. Clone Repository**
```bash
git clone https://github.com/YOUR-GITHUB-USERNAME/market-etl.git
cd market-etl
```

### **2. Configure AWS Credentials**
```bash
aws configure
# Enter your AWS Access Key ID and Secret Access Key
# Region: us-east-1
# Output format: json
```

### **3. Deploy Infrastructure**
```bash
cd infra/terraform

# Initialize Terraform
terraform init

# Preview changes
terraform plan

# Apply infrastructure
terraform apply
# Type 'yes' when prompted
```

### **4. Verify Deployment**
Check AWS Console:
- **S3:** Bucket `market-data-etl-dev-<account-id>` exists
- **Glue:** Database `market_data` exists
- **IAM:** Role `market-data-etl-dev-glue-job-role` exists

---

## Cost Estimate

**Current Infrastructure (M0):**
- S3 bucket (empty): $0.00/month
- Glue database: $0.00/month (catalog is free)
- IAM roles: $0.00/month (always free)

**Total: $0.00/month** 

**Projected Costs (M1-M2 with data):**
- S3 storage (50GB): ~$1.15/month
- Glue ETL (10 DPU-hours/month): ~$4.40/month
- Athena queries (10GB scanned): ~$0.05/month
- **Estimated: $5-10/month for dev scale**

**Cost Controls:**
- Lifecycle policies auto-archive cold data
- Partition pruning minimizes Athena scans
- Terraform enables easy teardown (`terraform destroy`)

---

## Project Roadmap

### **M0: Bootstrap & Infrastructure** (Complete)
- [x] AWS account and IAM user setup
- [x] S3 data lake with security and lifecycle policies
- [x] Glue Data Catalog database
- [x] IAM roles for Glue jobs with least privilege
- [x] Terraform IaC with provider locking
- [x] Git repository with branch protection
- [ ] Architecture diagram (deferred to end)

### **M1: Dev-Scale Pipeline** (In Progress)
- [ ] Glue PyShell downloader for Binance data (10-30GB)
- [ ] Glue Spark ETL: CSV → Parquet with data quality checks
- [ ] Partition by year/month/day/symbol
- [ ] Athena external tables and queries
- [ ] Airflow DAG for orchestration
- [ ] Initial performance metrics (rows/sec, scan efficiency)

### **M2: Scale Out** (Planned)
- [ ] Expand to 50 symbols × 3-5 years (hundreds of millions of rows)
- [ ] Tune Spark partitions and file sizes
- [ ] Advanced DQ checks (null guards, duplicate detection)
- [ ] Record comprehensive metrics (job duration, costs, partition counts)

### **M3: Warehouse Benchmark** (Optional)
- [ ] Load Parquet to Redshift Serverless
- [ ] Run TPC-like queries (VWAP, aggregations, joins)
- [ ] Compare Athena vs. Redshift performance

### **M4: Real-Time Replay** (Optional)
- [ ] Fargate producer: Parquet → Kinesis
- [ ] Consumer: Kinesis → S3 stream zone
- [ ] Demonstrate backpressure and fault tolerance

### **M5: Polish & Documentation** (Planned)
- [ ] Architecture diagram (full end-to-end)
- [ ] Cost analysis and optimization report
- [ ] Recruiter talking points document
- [ ] Teardown automation script

---

## Key Metrics (To Be Captured)

- **Volume:** Total rows, GB processed, partition counts
- **Throughput:** Rows/sec, GB/min per ETL stage  
- **Query Performance:** Athena p95 latency for common patterns
- **Cost Efficiency:** $/TB scanned, Glue DPU cost per GB

---

## Learning Outcomes

**Demonstrated Skills:**
- Cloud-native data engineering on AWS
- Infrastructure as Code with Terraform
- S3 data lake design (zones, partitioning, lifecycle)
- Serverless ETL with AWS Glue
- IAM security and least privilege
- SQL analytics with Athena
- Workflow orchestration with Airflow
- Cost optimization strategies
- Production DevOps practices (Git, PR workflow, IaC)

---

## Security & Best Practices

-  All S3 buckets have public access blocked
-  Server-side encryption enabled (AES256)
-  IAM roles use least privilege permissions
-  Credentials never committed to Git (`.gitignore` protection)
-  Resources tagged for cost tracking and management
-  Versioning enabled for data protection
-  Infrastructure managed via Terraform (reproducible, auditable)

---

##  License

This project is for educational and portfolio purposes.

---

**Status:** Active Development - Check back for M1 completion!