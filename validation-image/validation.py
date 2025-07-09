import logging
import boto3
import json
import os
from botocore.exceptions import ClientError
from io import StringIO
import csv

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# AWS Configuration
AWS_REGION = 'us-east-1'
S3_BUCKET = 'ecs.data'

# Get environment variables from Step Functions
TASK_TOKEN = os.environ.get('TASK_TOKEN')
FILE_KEY = os.environ.get('FILE_KEY')
EXECUTION_ID = os.environ.get('EXECUTION_ID')

# S3 Folder Paths
FOLDERS = {
    'orders': 'orders/',
    'order_items': 'order_items/',
    'products': 'products/'
}

QUARANTINE_FOLDERS = {
    'orders': 'quarantine/orders/',
    'order_items': 'quarantine/order_items/',
    'products': 'quarantine/products/'
}

# Schema Definitions
SCHEMAS = {
    'orders': ['order_id', 'user_id', 'created_at', 'status'],
    'order_items': ['id', 'order_id', 'product_id', 'sale_price'],
    'products': ['id', 'sku', 'cost', 'category', 'retail_price']
}

# Initialize AWS clients
s3_client = boto3.client('s3', region_name=AWS_REGION)
stepfunctions_client = boto3.client('stepfunctions', region_name=AWS_REGION)

def send_task_success(output):
    """Send success callback to Step Functions"""
    try:
        stepfunctions_client.send_task_success(
            taskToken=TASK_TOKEN,
            output=json.dumps(output)
        )
        logger.info("Task success sent to Step Functions")
    except Exception as e:
        logger.error(f"Error sending task success: {e}")

def send_task_failure(error, cause):
    """Send failure callback to Step Functions"""
    try:
        stepfunctions_client.send_task_failure(
            taskToken=TASK_TOKEN,
            error=error,
            cause=cause
        )
        logger.error(f"Task failure sent to Step Functions: {error}")
    except Exception as e:
        logger.error(f"Error sending task failure: {e}")

def list_s3_files(bucket_name, prefix):
    """List all CSV files in an S3 folder."""
    try:
        logger.info(f"Listing files in s3://{bucket_name}/{prefix}")
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            logger.info(f"No files found in s3://{bucket_name}/{prefix}")
            return []
        
        csv_files = []
        for obj in response['Contents']:
            key = obj['Key']
            # Skip folders and non-CSV files
            if not key.endswith('/') and key.lower().endswith('.csv'):
                csv_files.append(key)
        
        logger.info(f"Found {len(csv_files)} CSV files in s3://{bucket_name}/{prefix}")
        return csv_files
        
    except ClientError as e:
        logger.error(f"Error listing files in s3://{bucket_name}/{prefix}: {e}")
        return []

def read_s3_file_header(bucket_name, key):
    """Read only the header (first row) of a CSV file from S3."""
    try:
        logger.info(f"Reading header from s3://{bucket_name}/{key}")
        
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=key,
            Range='bytes=0-1023'  # Read first 1KB to get header
        )
        
        content = response['Body'].read().decode('utf-8')
        first_line = content.split('\n')[0].strip()
        
        csv_reader = csv.reader(StringIO(first_line))
        headers = next(csv_reader)
        headers = [col.strip() for col in headers]
        
        logger.info(f"Headers found: {headers}")
        return headers
        
    except ClientError as e:
        logger.error(f"Error reading header from s3://{bucket_name}/{key}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing header from s3://{bucket_name}/{key}: {e}")
        return None

def validate_schema(headers, schema_name):
    """Validate headers against a specific schema."""
    logger.info(f"Validating {schema_name} schema")
    
    validation_result = {
        'is_valid': True,
        'issues': [],
        'headers_found': headers,
        'mandatory_fields': SCHEMAS[schema_name]
    }
    
    if headers is None:
        validation_result['is_valid'] = False
        validation_result['issues'].append("Could not read file headers")
        return validation_result
    
    # Check if mandatory fields exist
    missing_fields = [field for field in SCHEMAS[schema_name] 
                     if field not in headers]
    if missing_fields:
        validation_result['is_valid'] = False
        validation_result['issues'].append(f"Missing mandatory fields: {missing_fields}")
    
    # Check for extra fields (informational)
    extra_fields = [field for field in headers 
                   if field not in SCHEMAS[schema_name]]
    if extra_fields:
        validation_result['issues'].append(f"Additional fields found: {extra_fields}")
    
    logger.info(f"{schema_name} schema validation completed. Issues found: {len(validation_result['issues'])}")
    return validation_result

def move_file_to_quarantine(bucket_name, source_key, quarantine_prefix):
    """Move a file from source location to quarantine folder."""
    try:
        filename = source_key.split('/')[-1]
        quarantine_key = quarantine_prefix + filename
        
        logger.info(f"Moving s3://{bucket_name}/{source_key} to s3://{bucket_name}/{quarantine_key}")
        
        # Copy file to quarantine
        s3_client.copy_object(
            CopySource={'Bucket': bucket_name, 'Key': source_key},
            Bucket=bucket_name,
            Key=quarantine_key
        )
        
        # Delete original file
        s3_client.delete_object(
            Bucket=bucket_name,
            Key=source_key
        )
        
        logger.info(f"Successfully moved file to quarantine: {quarantine_key}")
        return True
        
    except ClientError as e:
        logger.error(f"Error moving file to quarantine: {e}")
        return False

def determine_folder_type(file_key):
    """Determine which folder type based on the file key."""
    if file_key.endswith('/'):
        # It's a folder path, extract folder name
        folder_path = file_key.rstrip('/')
        folder_name = folder_path.split('/')[-1]
        return folder_name if folder_name in FOLDERS else None
    else:
        # It's a file path, determine from the path
        for folder_name, folder_prefix in FOLDERS.items():
            if file_key.startswith(folder_prefix):
                return folder_name
        return None

def process_folder(bucket_name, folder_name):
    """Process all files in a specific folder."""
    logger.info(f"Processing folder: {folder_name}")
    
    folder_prefix = FOLDERS[folder_name]
    quarantine_prefix = QUARANTINE_FOLDERS[folder_name]
    
    # List all files in the folder
    files = list_s3_files(bucket_name, folder_prefix)
    
    if not files:
        logger.info(f"No files to process in {folder_name}")
        return {
            'total_files': 0,
            'passed': 0,
            'failed': 0,
            'quarantined': 0,
            'processed_files': []
        }
    
    results = {
        'total_files': len(files),
        'passed': 0,
        'failed': 0,
        'quarantined': 0,
        'processed_files': []
    }
    
    for file_key in files:
        logger.info(f"Processing file: {file_key}")
        
        file_result = {
            'file_key': file_key,
            'status': 'unknown',
            'issues': []
        }
        
        # Read file header
        headers = read_s3_file_header(bucket_name, file_key)
        
        # Validate schema
        validation_result = validate_schema(headers, folder_name)
        
        if validation_result['is_valid']:
            logger.info(f"✓ File passed validation: {file_key}")
            results['passed'] += 1
            file_result['status'] = 'passed'
        else:
            logger.warning(f"✗ File failed validation: {file_key}")
            logger.warning(f"Issues: {validation_result['issues']}")
            results['failed'] += 1
            file_result['status'] = 'failed'
            file_result['issues'] = validation_result['issues']
            
            # Move to quarantine
            if move_file_to_quarantine(bucket_name, file_key, quarantine_prefix):
                results['quarantined'] += 1
                file_result['status'] = 'quarantined'
        
        results['processed_files'].append(file_result)
    
    # Log summary for this folder
    logger.info(f"Folder {folder_name} processing complete:")
    logger.info(f"  Total files: {results['total_files']}")
    logger.info(f"  Passed: {results['passed']}")
    logger.info(f"  Failed: {results['failed']}")
    logger.info(f"  Quarantined: {results['quarantined']}")
    
    return results

def main():
    """Main function to validate files and send results to Step Functions."""
    logger.info("Starting S3 file validation process")
    logger.info(f"Execution ID: {EXECUTION_ID}")
    logger.info(f"File Key: {FILE_KEY}")
    
    try:
        # Determine which folder to process
        folder_name = determine_folder_type(FILE_KEY)
        
        if not folder_name:
            error_msg = f"Could not determine folder type from file key: {FILE_KEY}"
            logger.error(error_msg)
            send_task_failure("ValidationError", error_msg)
            return
        
        logger.info(f"Processing folder type: {folder_name}")
        
        # Process the folder
        results = process_folder(S3_BUCKET, folder_name)
        
        # Prepare metadata for Step Functions
        metadata = {
            'folder_type': folder_name,
            'validation_results': results,
            'execution_id': EXECUTION_ID,
            'timestamp': str(boto3.Session().region_name)
        }
        
        # Check if validation was successful overall
        if results['failed'] > 0:
            logger.warning(f"Validation completed with {results['failed']} failed files")
            # Still send success but with warning metadata
            metadata['status'] = 'partial_success'
            metadata['warning'] = f"{results['failed']} files failed validation and were quarantined"
        else:
            metadata['status'] = 'success'
        
        # Send success to Step Functions
        send_task_success(metadata)
        
        logger.info("="*50)
        logger.info("VALIDATION PROCESS COMPLETE")
        logger.info("="*50)
        logger.info(f"Total files processed: {results['total_files']}")
        logger.info(f"Files passed validation: {results['passed']}")
        logger.info(f"Files failed validation: {results['failed']}")
        logger.info(f"Files moved to quarantine: {results['quarantined']}")
        logger.info("="*50)
        
    except Exception as e:
        error_msg = f"Unexpected error during validation: {str(e)}"
        logger.error(error_msg)
        send_task_failure("ValidationError", error_msg)

if __name__ == "__main__":
    main()
