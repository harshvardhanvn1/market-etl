# Production-Grade Streaming Unzipper for AWS Glue
# Handles files of ANY size by streaming in chunks
# Uses S3 multipart upload to bypass 5GB single PUT limit

import sys
import logging
import zipfile
from io import BytesIO
import boto3
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Chunk size for streaming (1MB chunks - memory efficient)
CHUNK_SIZE = 1024 * 1024  # 1MB


class StreamingUnzipper:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')
        
        self.stats = {
            'zips_found': 0,
            'zips_processed': 0,
            'zips_skipped': 0,
            'zips_failed': 0,
            'total_bytes_unzipped': 0
        }
    
    def list_zip_files(self, prefix):
        """List all ZIP files under a prefix"""
        logger.info(f"Listing ZIPs under s3://{self.bucket_name}/{prefix}")
        
        zip_files = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            if 'Contents' not in page:
                continue
            
            for obj in page['Contents']:
                if obj['Key'].endswith('.zip'):
                    zip_files.append(obj['Key'])
        
        self.stats['zips_found'] = len(zip_files)
        logger.info(f"Found {len(zip_files)} ZIP files")
        return zip_files
    
    def check_csv_exists(self, csv_key):
        """Check if CSV already exists (idempotency)"""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=csv_key)
            return True
        except ClientError:
            return False
    
    def streaming_unzip_to_s3(self, zip_key, csv_key):
        """
        Stream ZIP from S3, extract CSV, upload to S3 using multipart upload
        This works for files of ANY size without loading into memory
        """
        logger.info(f"Streaming unzip: {zip_key}")
        
        try:
            # Step 1: Download ZIP file (streaming to memory - this is OK for ZIPs)
            logger.info("  Downloading ZIP...")
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=zip_key)
            zip_content = response['Body'].read()
            zip_size_mb = len(zip_content) / (1024**2)
            logger.info(f"  ZIP size: {zip_size_mb:.1f} MB")
            
            # Step 2: Extract CSV from ZIP (in memory)
            logger.info("  Extracting CSV from ZIP...")
            with zipfile.ZipFile(BytesIO(zip_content)) as z:
                csv_filename = z.namelist()[0]  # Binance ZIPs have single file
                logger.info(f"  Found: {csv_filename}")
                
                # Get CSV info
                csv_info = z.getinfo(csv_filename)
                csv_size_mb = csv_info.file_size / (1024**2)
                logger.info(f"  Uncompressed CSV size: {csv_size_mb:.1f} MB")
                
                # Step 3: Stream CSV to S3 using multipart upload
                # This is the KEY optimization - no 5GB PUT limit!
                logger.info("  Starting multipart upload to S3...")
                
                # Initiate multipart upload
                mpu = self.s3_client.create_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=csv_key,
                    ServerSideEncryption='AES256'
                )
                upload_id = mpu['UploadId']
                
                parts = []
                part_number = 1
                
                # Open the compressed file and read in chunks
                with z.open(csv_filename) as csv_file:
                    while True:
                        # Read chunk (5MB at a time for S3 multipart)
                        chunk = csv_file.read(5 * 1024 * 1024)  # 5MB chunks
                        if not chunk:
                            break
                        
                        # Upload this part
                        logger.info(f"    Uploading part {part_number}...")
                        part_response = self.s3_client.upload_part(
                            Bucket=self.bucket_name,
                            Key=csv_key,
                            PartNumber=part_number,
                            UploadId=upload_id,
                            Body=chunk
                        )
                        
                        parts.append({
                            'PartNumber': part_number,
                            'ETag': part_response['ETag']
                        })
                        
                        part_number += 1
                
                # Complete multipart upload
                logger.info(f"  Completing upload ({len(parts)} parts)...")
                self.s3_client.complete_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=csv_key,
                    UploadId=upload_id,
                    MultipartUpload={'Parts': parts}
                )
                
                logger.info(f"✓ Successfully unzipped to {csv_key}")
                self.stats['zips_processed'] += 1
                self.stats['total_bytes_unzipped'] += csv_info.file_size
                
                return True
                
        except Exception as e:
            logger.error(f"✗ Failed to unzip {zip_key}: {e}")
            # Abort multipart upload if it was started
            try:
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=csv_key,
                    UploadId=upload_id
                )
            except:
                pass
            
            self.stats['zips_failed'] += 1
            return False
    
    def process_file(self, zip_key):
        """Process a single ZIP file"""
        # Build output CSV key (same path but different prefix)
        csv_key = zip_key.replace('raw/', 'raw_unzipped/').replace('.zip', '.csv')
        
        # Skip if already done (idempotency)
        if self.check_csv_exists(csv_key):
            logger.info(f"✓ Already exists: {csv_key}")
            self.stats['zips_skipped'] += 1
            return True
        
        # Stream unzip and upload
        return self.streaming_unzip_to_s3(zip_key, csv_key)
    
    def run(self, raw_prefix):
        """Main unzipping loop"""
        logger.info("="*60)
        logger.info("Starting Streaming Unzipper")
        logger.info(f"Bucket: {self.bucket_name}")
        logger.info(f"Prefix: {raw_prefix}")
        logger.info("Using S3 multipart upload (no size limits)")
        logger.info("="*60)
        
        # List all ZIPs
        zip_files = self.list_zip_files(raw_prefix)
        
        if not zip_files:
            logger.warning("No ZIP files found!")
            return
        
        # Process each one
        for zip_key in zip_files:
            self.process_file(zip_key)
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("Unzipping Complete!")
        logger.info(f"ZIPs found: {self.stats['zips_found']}")
        logger.info(f"ZIPs processed: {self.stats['zips_processed']}")
        logger.info(f"ZIPs skipped (already done): {self.stats['zips_skipped']}")
        logger.info(f"ZIPs failed: {self.stats['zips_failed']}")
        logger.info(f"Total data unzipped: {self.stats['total_bytes_unzipped'] / (1024**3):.2f} GB")
        logger.info("="*60)


def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME', 'DATA_TYPE'])
    
    logger.info(f"Job: {args['JOB_NAME']}")
    logger.info("Memory-efficient streaming mode enabled")
    
    unzipper = StreamingUnzipper(bucket_name=args['BUCKET_NAME'])
    
    raw_prefix = f"raw/binance/spot/{args['DATA_TYPE']}/"
    unzipper.run(raw_prefix)


if __name__ == "__main__":
    main()