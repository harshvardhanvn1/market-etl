# Glue Spark ETL Job - Binance Trades to Parquet
# Reads ZIP files from S3, validates data, writes partitioned Parquet
# Version 8: Uses explicit file listing to avoid pattern matching issues

import sys
import logging
from datetime import datetime
from io import BytesIO
import zipfile

import boto3
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when, input_file_name, regexp_extract
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, TimestampType, BooleanType, DateType, IntegerType
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TradesETL:
    def __init__(self, spark, glue_context, bucket_name):
        self.spark = spark
        self.glue_context = glue_context
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')
        
        # Statistics tracking
        self.stats = {
            'files_found': 0,
            'files_processed': 0,
            'files_failed': 0,
            'total_rows_read': 0,
            'total_rows_written': 0,
            'partitions_written': set()
        }
    
    def list_zip_files(self, prefix):
        """
        List all ZIP files under a given S3 prefix
        Returns list of full S3 paths
        """
        logger.info(f"Listing ZIP files under s3://{self.bucket_name}/{prefix}")
        
        zip_files = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        try:
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith('.zip'):
                        s3_path = f"s3://{self.bucket_name}/{key}"
                        zip_files.append(s3_path)
                        logger.info(f"Found: {s3_path}")
        
        except Exception as e:
            logger.error(f"Error listing S3 objects: {e}")
            raise
        
        self.stats['files_found'] = len(zip_files)
        logger.info(f"Total ZIP files found: {len(zip_files)}")
        return zip_files
    
    def get_binance_trades_schema(self):
        """
        Define the schema for Binance trades CSV
        Note: Binance CSVs have NO header row
        """
        return StructType([
            StructField("trade_id", LongType(), False),
            StructField("price", DoubleType(), False),
            StructField("quantity", DoubleType(), False),
            StructField("quote_qty", DoubleType(), False),
            StructField("time", LongType(), False),  # Unix timestamp in milliseconds
            StructField("is_buyer_maker", StringType(), False),  # "True" or "False" as strings
            StructField("is_best_match", StringType(), False)    # "True" or "False" as strings
        ])
    
    def read_single_zip_file(self, s3_path):
        """
        Read a single ZIP file from S3, extract CSV, and return DataFrame
        Uses RDD approach to handle ZIP decompression
        """
        logger.info(f"Reading ZIP file: {s3_path}")
        
        try:
            # Extract bucket and key from s3:// path
            path_parts = s3_path.replace("s3://", "").split("/", 1)
            bucket = path_parts[0]
            key = path_parts[1]
            
            # Download ZIP file content
            logger.info(f"Downloading {key}...")
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            zip_content = response['Body'].read()
            
            # Extract CSV from ZIP
            logger.info(f"Extracting CSV from ZIP...")
            with zipfile.ZipFile(BytesIO(zip_content)) as z:
                # Binance ZIPs contain a single CSV file
                csv_filename = z.namelist()[0]
                logger.info(f"Found CSV: {csv_filename}")
                csv_content = z.read(csv_filename).decode('utf-8')
            
            # Create RDD from CSV lines
            lines = csv_content.strip().split('\n')
            logger.info(f"CSV has {len(lines)} lines")
            
            rdd = self.spark.sparkContext.parallelize(lines)
            
            # Parse CSV with schema
            schema = self.get_binance_trades_schema()
            df = self.spark.read.csv(
                rdd,
                schema=schema,
                header=False  # Binance CSVs have no header
            )
            
            # Extract metadata from S3 path for partitioning
            # Path format: .../symbol=BTCUSDT/year=2025/month=07/BTCUSDT-trades-2025-07.zip
            symbol_match = key.split('symbol=')[1].split('/')[0] if 'symbol=' in key else 'UNKNOWN'
            year_match = key.split('year=')[1].split('/')[0] if 'year=' in key else '0'
            month_match = key.split('month=')[1].split('/')[0] if 'month=' in key else '0'
            
            # Add metadata columns
            df = df.withColumn("symbol", col("trade_id").cast("string") * 0 + symbol_match)  # Trick to create constant column
            df = df.withColumn("year_partition", col("trade_id").cast("string") * 0 + year_match)
            df = df.withColumn("month_partition", col("trade_id").cast("string") * 0 + month_match)
            
            row_count = df.count()
            logger.info(f"Parsed {row_count} rows from {s3_path}")
            
            self.stats['files_processed'] += 1
            self.stats['total_rows_read'] += row_count
            
            return df
            
        except Exception as e:
            logger.error(f"ERROR reading {s3_path}: {e}")
            self.stats['files_failed'] += 1
            return None
    
    def transform_trades_df(self, df):
        """
        Apply transformations and data quality checks
        """
        logger.info("Applying transformations...")
        
        # Convert timestamp from milliseconds to seconds, then to timestamp
        df = df.withColumn(
            "trade_time",
            to_timestamp(col("time") / 1000)
        )
        
        # Convert string booleans to actual booleans
        df = df.withColumn(
            "is_buyer_maker_bool",
            when(col("is_buyer_maker") == "True", True).otherwise(False)
        )
        df = df.withColumn(
            "is_best_match_bool",
            when(col("is_best_match") == "True", True).otherwise(False)
        )
        
        # Add load date (current date)
        df = df.withColumn(
            "load_dt",
            col("trade_time").cast(DateType())
        )
        
        # Extract day from timestamp for partitioning
        df = df.withColumn(
            "day",
            col("trade_time").cast(DateType()).cast(StringType()).substr(9, 2).cast(IntegerType())
        )
        
        # Cast partition columns to integers
        df = df.withColumn("year", col("year_partition").cast(IntegerType()))
        df = df.withColumn("month", col("month_partition").cast(IntegerType()))
        
        # Select final columns in desired order
        df = df.select(
            "trade_id",
            "trade_time",
            "symbol",
            "price",
            "quantity",
            "quote_qty",
            col("is_buyer_maker_bool").alias("is_buyer_maker"),
            col("is_best_match_bool").alias("is_best_match"),
            "load_dt",
            "year",
            "month",
            "day"
        )
        
        # Data quality checks
        initial_count = df.count()
        logger.info(f"Initial row count: {initial_count}")
        
        # Remove nulls in critical columns
        df = df.filter(
            col("trade_time").isNotNull() &
            col("price").isNotNull() &
            col("quantity").isNotNull()
        )
        
        # Remove negative prices/quantities
        df = df.filter(
            (col("price") > 0) &
            (col("quantity") > 0)
        )
        
        final_count = df.count()
        removed = initial_count - final_count
        logger.info(f"Removed {removed} rows in DQ checks")
        logger.info(f"Final row count: {final_count}")
        
        return df
    
    def write_partitioned_parquet(self, df, output_path):
        """
        Write DataFrame as partitioned Parquet files
        """
        logger.info(f"Writing Parquet to {output_path}")
        logger.info("Partitioning by: year, month, day, symbol")
        
        try:
            df.write \
                .mode("overwrite") \
                .partitionBy("year", "month", "day", "symbol") \
                .parquet(output_path)
            
            # Track partitions written
            partitions = df.select("year", "month", "day", "symbol").distinct().collect()
            for row in partitions:
                partition_key = f"year={row.year}/month={row.month}/day={row.day}/symbol={row.symbol}"
                self.stats['partitions_written'].add(partition_key)
            
            self.stats['total_rows_written'] = df.count()
            logger.info(f"Successfully wrote {self.stats['total_rows_written']} rows")
            logger.info(f"Created {len(self.stats['partitions_written'])} partitions")
            
        except Exception as e:
            logger.error(f"Error writing Parquet: {e}")
            raise
    
    def run(self, raw_prefix, output_prefix):
        """
        Main ETL logic
        """
        logger.info("="*60)
        logger.info("Starting Trades ETL Job")
        logger.info(f"Input: s3://{self.bucket_name}/{raw_prefix}")
        logger.info(f"Output: s3://{self.bucket_name}/{output_prefix}")
        logger.info("="*60)
        
        # Step 1: List all ZIP files
        zip_files = self.list_zip_files(raw_prefix)
        
        if not zip_files:
            logger.error("No ZIP files found! Exiting.")
            return False
        
        # Step 2: Read and union all ZIP files
        all_dfs = []
        for zip_path in zip_files:
            df = self.read_single_zip_file(zip_path)
            if df is not None:
                all_dfs.append(df)
        
        if not all_dfs:
            logger.error("No DataFrames created! All files failed.")
            return False
        
        logger.info(f"Combining {len(all_dfs)} DataFrames...")
        combined_df = all_dfs[0]
        for df in all_dfs[1:]:
            combined_df = combined_df.union(df)
        
        logger.info(f"Combined DataFrame has {combined_df.count()} rows")
        
        # Step 3: Transform
        transformed_df = self.transform_trades_df(combined_df)
        
        # Step 4: Write Parquet
        output_path = f"s3://{self.bucket_name}/{output_prefix}"
        self.write_partitioned_parquet(transformed_df, output_path)
        
        # Print summary
        logger.info("\n" + "="*60)
        logger.info("ETL Complete!")
        logger.info(f"Files found: {self.stats['files_found']}")
        logger.info(f"Files processed: {self.stats['files_processed']}")
        logger.info(f"Files failed: {self.stats['files_failed']}")
        logger.info(f"Total rows read: {self.stats['total_rows_read']}")
        logger.info(f"Total rows written: {self.stats['total_rows_written']}")
        logger.info(f"Partitions created: {len(self.stats['partitions_written'])}")
        logger.info("="*60)
        
        return True


# Main entry point
def main():
    # Get job parameters
    args = getResolvedOptions(
        sys.argv,
        ['JOB_NAME', 'BUCKET_NAME', 'DATA_TYPE']
    )
    
    logger.info(f"Job started: {args['JOB_NAME']}")
    logger.info(f"Bucket: {args['BUCKET_NAME']}")
    logger.info(f"Data type: {args['DATA_TYPE']}")
    
    # Initialize Spark and Glue contexts
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    
    # Configure Spark for better performance
    spark.conf.set("spark.sql.shuffle.partitions", "100")
    spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB
    
    # Run ETL
    etl = TradesETL(spark, glue_context, args['BUCKET_NAME'])
    
    raw_prefix = f"raw/binance/spot/{args['DATA_TYPE']}/"
    output_prefix = f"processed/binance/spot/{args['DATA_TYPE']}/"
    
    success = etl.run(raw_prefix, output_prefix)
    
    if success:
        logger.info("Job completed successfully!")
        job.commit()
    else:
        logger.error("Job failed!")
        raise Exception("ETL job failed - see logs for details")


if __name__ == "__main__":
    main()