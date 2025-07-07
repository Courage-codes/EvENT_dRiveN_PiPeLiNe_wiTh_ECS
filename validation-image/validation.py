import logging
import boto3
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

