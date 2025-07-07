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

