# Glue Spark ETL - Binance Trades CSV to Parquet
# Reads unzipped CSVs from S3, transforms, writes partitioned Parquet
# Production-grade: Works with streaming unzipper for unlimited scalability

import sys
import logging

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when, regexp_extract, input_file_name, dayofmonth
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, DateType, IntegerType
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_binance_trades_schema():
    """
    Schema for Binance trades CSV (no headers)
    """
    return StructType([
        StructField("trade_id", LongType(), False),
        StructField("price", DoubleType(), False),
        StructField("quantity", DoubleType(), False),
        StructField("quote_qty", DoubleType(), False),
        StructField("time", LongType(), False),
        StructField("is_buyer_maker", StringType(), False),
        StructField("is_best_match", StringType(), False)
    ])


def main():
    # Get parameters
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME'])
    
    logger.info("="*60)
    logger.info(f"Job: {args['JOB_NAME']}")
    logger.info(f"Bucket: {args['BUCKET_NAME']}")
    logger.info("="*60)
    
    # Initialize Spark
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    
    # Configure Spark for better performance
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB
    
    # Build paths
    bucket = args['BUCKET_NAME']
    input_path = f"s3://{bucket}/raw_unzipped/binance/spot/trades/"
    output_path = f"s3://{bucket}/processed/binance/spot/trades/"
    
    logger.info(f"Input: {input_path}")
    logger.info(f"Output: {output_path}")
    
    # Read all CSVs with Spark's native distributed reading
    logger.info("Reading CSVs from S3 (distributed)...")
    
    schema = get_binance_trades_schema()
    
    df = spark.read \
        .option("recursiveFileLookup", "true") \
        .schema(schema) \
        .csv(input_path)
    
    logger.info(f"Loaded DataFrame with {len(df.columns)} columns")
    
    # Add input file path for extracting metadata
    df = df.withColumn("_input_file", input_file_name())
    
    # Extract symbol, year, month from file path
    # Path: s3://.../symbol=BTCUSDT/year=2025/month=07/BTCUSDT-trades-2025-07.csv
    df = df.withColumn("symbol", regexp_extract(col("_input_file"), r"symbol=([A-Z]+)/", 1))
    df = df.withColumn("year", regexp_extract(col("_input_file"), r"year=(\d+)/", 1).cast(IntegerType()))
    df = df.withColumn("month", regexp_extract(col("_input_file"), r"month=(\d+)/", 1).cast(IntegerType()))
    
    # Convert timestamp from milliseconds to timestamp
    df = df.withColumn("trade_time", to_timestamp(col("time") / 1000))
    
    # Extract day for partitioning using proper Spark function
    df = df.withColumn("day", dayofmonth(col("trade_time")))
    
    # Convert string booleans to actual booleans
    df = df.withColumn(
        "is_buyer_maker_bool",
        when(col("is_buyer_maker") == "True", True).otherwise(False)
    )
    df = df.withColumn(
        "is_best_match_bool",
        when(col("is_best_match") == "True", True).otherwise(False)
    )
    
    # Add load date
    df = df.withColumn("load_dt", col("trade_time").cast(DateType()))
    
    # Select final columns
    df_final = df.select(
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
    logger.info("Applying data quality checks...")
    initial_count = df_final.count()
    logger.info(f"Initial row count: {initial_count:,}")
    
    # Filter for valid data
    df_clean = df_final.filter(
        # Timestamp must not be NULL
        col("trade_time").isNotNull() &
        
        # Price and quantity must exist and be positive
        col("price").isNotNull() &
        col("quantity").isNotNull() &
        (col("price") > 0) &
        (col("quantity") > 0) &
        
        # CRITICAL: Validate the extracted date components are possible
        # Reject impossible dates like Sept 31, Feb 30, Feb 29 in non-leap years, etc.
        (
            # Valid day ranges by month
            (
                # Months with 31 days: Jan, Mar, May, Jul, Aug, Oct, Dec
                ((col("month").isin(1, 3, 5, 7, 8, 10, 12)) & (col("day") <= 31)) |
                
                # Months with 30 days: Apr, Jun, Sep, Nov
                ((col("month").isin(4, 6, 9, 11)) & (col("day") <= 30)) |
                
                # February: Requires leap year logic
                (
                    (col("month") == 2) &
                    (
                        # Leap years: Feb 1-29 allowed
                        (
                            (
                                ((col("year") % 4 == 0) & (col("year") % 100 != 0)) |
                                (col("year") % 400 == 0)
                            ) &
                            (col("day") <= 29)
                        ) |
                        # Non-leap years: Feb 1-28 allowed
                        (
                            (
                                (col("year") % 4 != 0) |
                                ((col("year") % 100 == 0) & (col("year") % 400 != 0))
                            ) &
                            (col("day") <= 28)
                        )
                    )
                )
            ) &
            # Day must be at least 1
            (col("day") >= 1)
        )
    )
    
    final_count = df_clean.count()
    removed = initial_count - final_count
    logger.info(f"Removed {removed:,} rows in DQ checks ({removed/initial_count*100:.2f}%)")
    logger.info(f"Final row count: {final_count:,}")
    
    # Show sample data
    logger.info("Sample data:")
    df_clean.show(5, truncate=False)
    
    # Show partition distribution
    logger.info("Partition distribution:")
    df_clean.groupBy("year", "month", "day", "symbol") \
        .count() \
        .orderBy("year", "month", "day", "symbol") \
        .show(100)
    
    # Write partitioned Parquet
    logger.info(f"Writing Parquet to {output_path}")
    logger.info("Partitioning by: year, month, day, symbol")
    
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day", "symbol") \
        .parquet(output_path)
    
    logger.info("="*60)
    logger.info("ETL Complete!")
    logger.info(f"Rows processed: {final_count:,}")
    logger.info(f"Output: {output_path}")
    logger.info("="*60)
    
    job.commit()


if __name__ == "__main__":
    main()