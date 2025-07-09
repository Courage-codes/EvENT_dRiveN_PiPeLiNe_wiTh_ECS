import logging
import boto3
from botocore.exceptions import ClientError
from io import StringIO
import csv
import json

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# AWS Configuration
AWS_REGION = 'us-east-1'
S3_BUCKET = 'ecs.data'

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

# Initialize S3 client
s3_client = boto3.client('s3', region_name=AWS_REGION)

def list_s3_files(bucket_name, prefix):
    """
    List all CSV files in an S3 folder.
    """
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
    """
    Read only the header (first row) of a CSV file from S3.
    """
    try:
        logger.info(f"Reading header from s3://{bucket_name}/{key}")
        
        # Get the first few bytes to read just the header
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=key,
            Range='bytes=0-1023'  # Read first 1KB to get header
        )
        
        content = response['Body'].read().decode('utf-8')
        
        # Find the first line (header)
        first_line = content.split('\n')[0].strip()
        
        # Parse CSV header properly
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
    """
    Validate headers against a specific schema.
    """
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
    """
    Move a file from source location to quarantine folder.
    """
    try:
        # Extract filename from source key
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


def process_folder(bucket_name, folder_name):
    """
    Process all files in a specific folder.
    """
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
            'file_name': file_key,
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
                file_result['quarantined'] = True
            else:
                file_result['quarantined'] = False
        
        results['processed_files'].append(file_result)
    
    # Log summary for this folder
    logger.info(f"Folder {folder_name} processing complete:")
    logger.info(f"  Total files: {results['total_files']}")
    logger.info(f"  Passed: {results['passed']}")
    logger.info(f"  Failed: {results['failed']}")
    logger.info(f"  Quarantined: {results['quarantined']}")
    
    return results


def main():
    """
    Main function to validate all files in all folders.
    """
    logger.info("Starting S3 file validation process")
    
    overall_results = {
        'total_files': 0,
        'passed': 0,
        'failed': 0,
        'quarantined': 0,
        'folder_results': {}
    }
    
    # Process each folder
    for folder_name in FOLDERS.keys():
        try:
            results = process_folder(S3_BUCKET, folder_name)
            overall_results['folder_results'][folder_name] = results
            
            if results:
                overall_results['total_files'] += results['total_files']
                overall_results['passed'] += results['passed']
                overall_results['failed'] += results['failed']
                overall_results['quarantined'] += results['quarantined']
        except Exception as e:
            logger.error(f"Error processing folder {folder_name}: {e}")
            overall_results['folder_results'][folder_name] = {
                'error': str(e),
                'total_files': 0,
                'passed': 0,
                'failed': 0,
                'quarantined': 0
            }
    
    # Final summary
    logger.info("="*50)
    logger.info("VALIDATION PROCESS COMPLETE")
    logger.info("="*50)
    logger.info(f"Total files processed: {overall_results['total_files']}")
    logger.info(f"Files passed validation: {overall_results['passed']}")
    logger.info(f"Files failed validation: {overall_results['failed']}")
    logger.info(f"Files moved to quarantine: {overall_results['quarantined']}")
    logger.info("="*50)
    
    # Determine overall success and create response
    success = overall_results['failed'] == 0 and overall_results['total_files'] > 0
    
    # Determine status code and message
    if overall_results['total_files'] == 0:
        status_code = 204  # No Content
        message = "No files found to process"
    elif overall_results['failed'] == 0:
        status_code = 200  # Success
        message = f"All {overall_results['total_files']} files passed validation successfully"
    elif overall_results['passed'] > 0:
        status_code = 206  # Partial Content
        message = f"{overall_results['failed']} out of {overall_results['total_files']} files failed validation and were quarantined"
    else:
        status_code = 400  # Bad Request
        message = f"All {overall_results['total_files']} files failed validation"
    
    response = {
        'statusCode': status_code,
        'success': success,
        'message': message,
        'results': {
            'total_files': overall_results['total_files'],
            'passed': overall_results['passed'],
            'failed': overall_results['failed'],
            'quarantined': overall_results['quarantined'],
            'success_rate': round((overall_results['passed'] / overall_results['total_files'] * 100), 2) if overall_results['total_files'] > 0 else 0
        },
        'folder_details': overall_results['folder_results']
    }
    
    logger.info(f"Lambda response: {json.dumps(response, indent=2)}")
    return response


def lambda_handler(event, context):
    """
    AWS Lambda handler for Step Functions integration.
    """
    try:
        logger.info(f"Lambda started with event: {json.dumps(event)}")
        logger.info(f"Lambda context: {context}")
        
        # Extract any parameters from event if needed
        bucket_override = event.get('bucket', S3_BUCKET)
        if bucket_override != S3_BUCKET:
            global S3_BUCKET
            S3_BUCKET = bucket_override
            logger.info(f"Using bucket override: {S3_BUCKET}")
        
        # Run the main validation process
        result = main()
        
        # Add execution metadata
        result['execution_info'] = {
            'request_id': context.aws_request_id if context else 'local-execution',
            'function_name': context.function_name if context else 'local-function',
            'function_version': context.function_version if context else 'local-version',
            'remaining_time_ms': context.get_remaining_time_in_millis() if context else None
        }
        
        logger.info("Lambda execution completed successfully")
        return result
        
    except Exception as e:
        error_message = f"Validation process failed: {str(e)}"
        logger.error(error_message, exc_info=True)
        
        error_response = {
            'statusCode': 500,
            'success': False,
            'message': error_message,
            'results': None,
            'execution_info': {
                'request_id': context.aws_request_id if context else 'local-execution',
                'function_name': context.function_name if context else 'local-function',
                'error': str(e)
            }
        }
        
        return error_response


if __name__ == "__main__":
    # For local testing
    class MockContext:
        aws_request_id = "local-test-123"
        function_name = "s3-file-validator"
        function_version = "1.0"
        
        def get_remaining_time_in_millis(self):
            return 30000
    
    # Test the lambda handler
    test_event = {}
    test_context = MockContext()
    result = lambda_handler(test_event, test_context)
    print(json.dumps(result, indent=2))
