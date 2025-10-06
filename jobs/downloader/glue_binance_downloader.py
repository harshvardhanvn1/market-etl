# Binance Data Downloader for AWS Glue
# Downloads historical market data from Binance and uploads to S3
# Uses streaming to avoid filling up local disk

import sys
import logging
import time
from datetime import datetime
import boto3
import requests
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions

# Setup logging so we can see what's happening in CloudWatch
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Base URL for Binance public data
BINANCE_BASE_URL = "https://data.binance.vision/data/spot/monthly"


class BinanceDownloader:
    def __init__(self, bucket_name, max_retries=3):
        self.bucket_name = bucket_name
        self.max_retries = max_retries
        self.s3_client = boto3.client('s3')
        self.session = requests.Session()
        
        # Keep track of what happens
        self.stats = {
            'attempted': 0,
            'downloaded': 0,
            'skipped': 0,
            'failed': 0,
            'total_bytes': 0
        }
    
    def generate_months(self, start_month, end_month):
        # Takes "2025-07" and "2025-09" and gives you all months in between
        # Returns list like [(2025, 7), (2025, 8), (2025, 9)]
        start_year, start_mon = map(int, start_month.split('-'))
        end_year, end_mon = map(int, end_month.split('-'))
        
        start_date = datetime(start_year, start_mon, 1)
        end_date = datetime(end_year, end_mon, 1)
        
        months = []
        current = start_date
        while current <= end_date:
            months.append((current.year, current.month))
            # Jump to next month
            if current.month == 12:
                current = datetime(current.year + 1, 1, 1)
            else:
                current = datetime(current.year, current.month + 1, 1)
        
        return months
    
    def build_download_url(self, data_type, symbol, year, month):
        # Build the URL to download from Binance
        # Example: https://data.binance.vision/data/spot/monthly/trades/BTCUSDT/BTCUSDT-trades-2025-09.zip
        month_str = f"{month:02d}"  # 9 becomes "09"
        filename = f"{symbol}-{data_type}-{year}-{month_str}.zip"
        url = f"{BINANCE_BASE_URL}/{data_type}/{symbol}/{filename}"
        return url
    
    def build_s3_path(self, data_type, symbol, year, month):
        # Create the S3 path with Hive-style partitioning
        # This makes it easy for Athena to read later
        # Format: raw/binance/spot/trades/symbol=BTCUSDT/year=2025/month=09/filename.zip
        month_str = f"{month:02d}"
        filename = f"{symbol}-{data_type}-{year}-{month_str}.zip"
        
        s3_key = (
            f"raw/binance/spot/{data_type}/"
            f"symbol={symbol}/"
            f"year={year}/"
            f"month={month_str}/"
            f"{filename}"
        )
        return s3_key
    
    def check_if_exists(self, s3_key):
        # Check if we already downloaded this file
        # No point downloading the same thing twice!
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.warning(f"Error checking if file exists: {e}")
                return False
    
    def download_file(self, url, s3_key, symbol, year, month):
        # Main download logic - tries multiple times if it fails
        self.stats['attempted'] += 1
        
        # Skip if already downloaded (saves time and money)
        if self.check_if_exists(s3_key):
            logger.info(f"Already have {symbol} {year}-{month:02d}, skipping")
            self.stats['skipped'] += 1
            return True
        
        # Try downloading with retries
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Downloading {symbol} {year}-{month:02d} (attempt {attempt + 1}/{self.max_retries})")
                
                # Download with streaming so we don't run out of memory
                response = self.session.get(url, stream=True, timeout=300)
                response.raise_for_status()
                
                # See how big the file is
                file_size = int(response.headers.get('Content-Length', 0))
                logger.info(f"File size: {file_size / (1024**2):.2f} MB")
                
                # Upload directly to S3 (streaming, no temp files!)
                self.s3_client.upload_fileobj(
                    response.raw,
                    self.bucket_name,
                    s3_key,
                    ExtraArgs={'ServerSideEncryption': 'AES256'}  # Encrypt it
                )
                
                logger.info(f"✓ Successfully uploaded {symbol} {year}-{month:02d}")
                self.stats['downloaded'] += 1
                self.stats['total_bytes'] += file_size
                return True
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    # File doesn't exist on Binance, don't retry
                    logger.warning(f"File not found on Binance: {symbol} {year}-{month:02d}")
                    self.stats['failed'] += 1
                    return False
                else:
                    logger.error(f"HTTP error: {e}")
                    
            except Exception as e:
                logger.error(f"Download error: {e}")
            
            # Wait before retrying (exponential backoff)
            if attempt < self.max_retries - 1:
                wait_time = 2 ** attempt  # 1, 2, 4 seconds
                logger.info(f"Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
        
        # All attempts failed
        logger.error(f"✗ Failed to download {symbol} {year}-{month:02d} after {self.max_retries} attempts")
        self.stats['failed'] += 1
        return False
    
    def run(self, symbols, start_month, end_month, data_type="trades"):
        # Download everything for all symbols and months
        logger.info("="*60)
        logger.info("Starting Binance Downloader")
        logger.info(f"Symbols: {symbols}")
        logger.info(f"Months: {start_month} to {end_month}")
        logger.info(f"Data type: {data_type}")
        logger.info(f"S3 bucket: {self.bucket_name}")
        logger.info("="*60)
        
        start_time = time.time()
        
        # Figure out how many files we need to download
        months = self.generate_months(start_month, end_month)
        total_files = len(symbols) * len(months)
        logger.info(f"Total files to download: {total_files}")
        
        # Download each symbol for each month
        for symbol in symbols:
            logger.info(f"\nProcessing {symbol}...")
            for year, month in months:
                url = self.build_download_url(data_type, symbol, year, month)
                s3_key = self.build_s3_path(data_type, symbol, year, month)
                
                self.download_file(url, s3_key, symbol, year, month)
                
                # Be nice to Binance servers, don't hammer them
                time.sleep(0.5)
        
        # Print summary
        elapsed = time.time() - start_time
        logger.info("\n" + "="*60)
        logger.info("Download Complete!")
        logger.info(f"Attempted: {self.stats['attempted']}")
        logger.info(f"Downloaded: {self.stats['downloaded']}")
        logger.info(f"Skipped (already existed): {self.stats['skipped']}")
        logger.info(f"Failed: {self.stats['failed']}")
        logger.info(f"Total data: {self.stats['total_bytes'] / (1024**3):.2f} GB")
        logger.info(f"Time: {elapsed / 60:.1f} minutes")
        logger.info("="*60)
        
        return self.stats


# Main entry point when Glue runs this script
def main():
    # Get parameters that were passed to the Glue job
    args = getResolvedOptions(
        sys.argv,
        ['JOB_NAME', 'BUCKET_NAME', 'SYMBOLS', 'START_YEAR_MONTH', 
         'END_YEAR_MONTH', 'DATA_TYPE']
    )
    
    logger.info(f"Job started: {args['JOB_NAME']}")
    
    # Parse comma-separated symbols into a list
    symbols = [s.strip() for s in args['SYMBOLS'].split(',')]
    
    # Create downloader and run it
    downloader = BinanceDownloader(bucket_name=args['BUCKET_NAME'])
    
    stats = downloader.run(
        symbols=symbols,
        start_month=args['START_YEAR_MONTH'],
        end_month=args['END_YEAR_MONTH'],
        data_type=args['DATA_TYPE']
    )
    
    # Log if there were any failures
    if stats['failed'] > 0:
        logger.warning(f"Completed with {stats['failed']} failures")
    else:
        logger.info("All files downloaded successfully!")


if __name__ == "__main__":
    main()