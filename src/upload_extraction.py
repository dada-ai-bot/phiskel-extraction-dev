#!/usr/bin/env python3
"""
Upload extraction JSONL files to S3
Triggered after extraction phase completes
"""

import argparse
import boto3
import json
import os
import sys
from pathlib import Path
from datetime import datetime


def upload_extraction_to_s3(
    output_dir: str,
    job_id: str,
    bucket: str,
    prefix: str,
    user_id: str,
    char_name: str,
    model_name: str
):
    """Upload all JSONL files from extraction output to S3"""
    
    s3_client = boto3.client('s3')
    output_path = Path(output_dir)
    
    if not output_path.exists():
        print(f"ERROR: Output directory does not exist: {output_path}")
        return False
    
    # Find all JSONL files
    jsonl_files = list(output_path.glob("*.jsonl"))
    
    if not jsonl_files:
        print(f"ERROR: No .jsonl files found in {output_path}")
        return False
    
    print(f"Found {len(jsonl_files)} JSONL file(s) to upload")
    
    # Upload each file
    s3_prefix = f"{prefix}/{job_id}"
    uploaded_files = []
    
    for jsonl_file in jsonl_files:
        s3_key = f"{s3_prefix}/{jsonl_file.name}"
        
        print(f"Uploading {jsonl_file.name} to s3://{bucket}/{s3_key}...")
        
        try:
            # Upload with metadata
            extra_args = {
                'ContentType': 'application/jsonl',
                'Metadata': {
                    'extraction-job-id': job_id,
                    'character-name': char_name,
                    'model-name': model_name,
                    'user-id': user_id,
                    'uploaded-at': datetime.utcnow().isoformat()
                }
            }
            
            s3_client.upload_file(
                str(jsonl_file),
                bucket,
                s3_key,
                ExtraArgs=extra_args
            )
            
            uploaded_files.append(s3_key)
            print(f"  ✓ Uploaded: {s3_key}")
            
        except Exception as e:
            print(f"  ✗ Failed to upload {jsonl_file.name}: {e}")
            return False
    
    # Upload metadata file
    metadata = {
        'jobId': job_id,
        'characterName': char_name,
        'modelName': model_name,
        'userId': user_id,
        'uploadedAt': datetime.utcnow().isoformat(),
        'fileCount': len(uploaded_files),
        'files': uploaded_files,
        's3Location': f"s3://{bucket}/{s3_prefix}/"
    }
    
    metadata_key = f"{s3_prefix}/metadata.json"
    print(f"Uploading metadata to s3://{bucket}/{metadata_key}...")
    
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2),
            ContentType='application/json',
            Metadata={
                'extraction-job-id': job_id,
                'character-name': char_name
            }
        )
        print(f"  ✓ Uploaded: {metadata_key}")
    except Exception as e:
        print(f"  ✗ Failed to upload metadata: {e}")
        return False
    
    print(f"\n✓ Successfully uploaded {len(uploaded_files)} file(s) to s3://{bucket}/{s3_prefix}/")
    return True


def main():
    parser = argparse.ArgumentParser(description='Upload extraction JSONL files to S3')
    parser.add_argument('--output-dir', required=True, help='Directory containing JSONL files')
    parser.add_argument('--job-id', required=True, help='Unique job identifier')
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--prefix', required=True, help='S3 prefix/folder')
    parser.add_argument('--user-id', required=True, help='User ID for metadata')
    parser.add_argument('--char-name', required=True, help='Character name')
    parser.add_argument('--model-name', required=True, help='Extraction model name')
    
    args = parser.parse_args()
    
    success = upload_extraction_to_s3(
        output_dir=args.output_dir,
        job_id=args.job_id,
        bucket=args.bucket,
        prefix=args.prefix,
        user_id=args.user_id,
        char_name=args.char_name,
        model_name=args.model_name
    )
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
