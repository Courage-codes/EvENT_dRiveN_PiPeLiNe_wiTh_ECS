import logging
import json
import time
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from pyspark.sql import SparkSession
import datetime
from pyspark.sql.functions import (
    col, to_date, sum as _sum, countDistinct, count, when, round as spark_round
)

# Configuration constants
AWS_REGION = 'us-east-1'
S3_BUCKET = 'ecs.data'
DATA_PATHS = {
    'orders': 'orders/',
    'order_items': 'order_items/',
    'products': 'products/'
}

# Initialize boto3 session
boto_session = boto3.Session(region_name=AWS_REGION)

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def setup_spark_with_s3():
    """Set up Spark session with S3A configuration"""
    try:
        spark = SparkSession.builder \
            .appName("ECSAnalytics") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.region", AWS_REGION) \
            .getOrCreate()
        logger.info("Initialized Spark session with S3A configuration")
        return spark
    except Exception as e:
        logger.exception("Failed to initialize Spark session: %s", e)
        return None
