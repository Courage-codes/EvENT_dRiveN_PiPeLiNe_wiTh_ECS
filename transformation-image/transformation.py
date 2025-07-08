import logging
import json
import time
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
import datetime
from pyspark.sql.functions import (
    col, to_date, sum as _sum, countDistinct, count, when, round as spark_round
)

# Configuration constants
AWS_REGION = 'us-east-1'
S3_BUCKET = 'ecs.data'
S3_ARCHIVE_BUCKET = 'ecs.archive'
DATA_PATHS = {
    'orders': 'orders/',
    'order_items': 'order_items/',
    'products': 'products/'
}

# DynamoDB table configurations
DYNAMODB_TABLES = {
    'category_metrics_table': {
        'AttributeDefinitions': [
            {'AttributeName': 'category', 'AttributeType': 'S'},
            {'AttributeName': 'order_date', 'AttributeType': 'S'}
        ],
        'KeySchema': [
            {'AttributeName': 'category', 'KeyType': 'HASH'},
            {'AttributeName': 'order_date', 'KeyType': 'RANGE'}
        ],
        'BillingMode': 'PAY_PER_REQUEST'
    },
    'order_metrics_table': {
        'AttributeDefinitions': [
            {'AttributeName': 'order_date', 'AttributeType': 'S'}
        ],
        'KeySchema': [
            {'AttributeName': 'order_date', 'KeyType': 'HASH'}
        ],
        'BillingMode': 'PAY_PER_REQUEST'
    }
}

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def create_dynamodb_table(table_name, table_config):
    """Create DynamoDB table if it doesn't exist"""
    try:
        dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
        
        # Check if table already exists
        try:
            table = dynamodb.Table(table_name)
            table.load()
            logger.info(f"Table {table_name} already exists")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                logger.error(f"Error checking table {table_name}: {e}")
                return False
        
        # Create the table
        logger.info(f"Creating DynamoDB table: {table_name}")
        table = dynamodb.create_table(
            TableName=table_name,
            **table_config
        )
        
        # Wait for table to be created
        table.wait_until_exists()
        logger.info(f"Table {table_name} created successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create table {table_name}: {e}")
        return False

def setup_dynamodb_tables():
    """Setup all required DynamoDB tables"""
    logger.info("Setting up DynamoDB tables...")
    
    for table_name, table_config in DYNAMODB_TABLES.items():
        if not create_dynamodb_table(table_name, table_config):
            logger.error(f"Failed to setup table {table_name}")
            return False
    
    logger.info("All DynamoDB tables are ready")
    return True

def setup_spark_session():
    """Set up Spark session with optimized S3 configuration"""
    try:
        spark = SparkSession.builder \
            .appName("ECSAnalytics") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.region", AWS_REGION) \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        logger.info("Spark session initialized successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to initialize Spark session: {e}")
        return None

def get_s3_files_to_process():
    """Get list of files to process from S3"""
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    files_to_process = {}
    
    try:
        for data_type, path in DATA_PATHS.items():
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET,
                Prefix=path
            )
            
            if 'Contents' in response:
                files_to_process[data_type] = [obj['Key'] for obj in response['Contents'] 
                                             if obj['Key'].endswith('.csv')]
            else:
                files_to_process[data_type] = []
        
        logger.info(f"Found files to process: {files_to_process}")
        return files_to_process
    except Exception as e:
        logger.error(f"Failed to list S3 files: {e}")
        return {}

def archive_processed_files(files_to_process):
    """Move processed files to archive bucket"""
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    
    moved_files = []
    failed_files = []
    
    try:
        for data_type, file_list in files_to_process.items():
            for file_key in file_list:
                try:
                    # Create archive key with timestamp
                    archive_key = f"processed/{timestamp}/{file_key}"
                    
                    # Copy file to archive bucket
                    copy_source = {'Bucket': S3_BUCKET, 'Key': file_key}
                    s3_client.copy_object(
                        CopySource=copy_source,
                        Bucket=S3_ARCHIVE_BUCKET,
                        Key=archive_key
                    )
                    
                    # Verify the copy succeeded by checking if file exists in archive
                    try:
                        s3_client.head_object(Bucket=S3_ARCHIVE_BUCKET, Key=archive_key)
                        
                        # Delete from original bucket only after successful copy verification
                        s3_client.delete_object(Bucket=S3_BUCKET, Key=file_key)
                        moved_files.append(file_key)
                        logger.info(f"Successfully moved {file_key} to archive")
                        
                    except ClientError as e:
                        logger.error(f"Failed to verify archived file {archive_key}: {e}")
                        failed_files.append(file_key)
                        
                except Exception as e:
                    logger.error(f"Failed to move file {file_key}: {e}")
                    failed_files.append(file_key)
        
        if moved_files:
            logger.info(f"Successfully moved {len(moved_files)} files to {S3_ARCHIVE_BUCKET}")
        
        if failed_files:
            logger.warning(f"Failed to move {len(failed_files)} files: {failed_files}")
            return len(failed_files) == 0  # Return False if any files failed
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to archive files: {e}")
        return False

def load_data_from_s3(spark):
    """Load data from S3 with selective caching"""
    try:
        orders_path = f"s3a://{S3_BUCKET}/{DATA_PATHS['orders']}"
        items_path = f"s3a://{S3_BUCKET}/{DATA_PATHS['order_items']}"
        products_path = f"s3a://{S3_BUCKET}/{DATA_PATHS['products']}"
        
        logger.info("Loading data from S3...")
        
        # Cache orders and items as they're used multiple times
        orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv(orders_path).cache()
        items_df = spark.read.option("header", "true").option("inferSchema", "true").csv(items_path).cache()
        
        # Don't cache products as it's only used once for joining
        products_df = spark.read.option("header", "true").option("inferSchema", "true").csv(products_path)

        logger.info("Data loaded successfully from S3")
        return orders_df, items_df, products_df

    except Exception as e:
        logger.error(f"Failed to load S3 data: {e}")
        return None, None, None

def transform_data(orders_df, items_df):
    """Transform data with necessary type conversions"""
    try:
        orders_df = orders_df.withColumn("order_date", to_date(col("created_at")))
        items_df = items_df.withColumn("sale_price", col("sale_price").cast("float"))
        
        logger.info("Data transformation completed")
        return orders_df, items_df
    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        return None, None

def calculate_category_metrics(items_df, orders_df, products_df):
    """Calculate daily revenue, average order value, and return rate by category"""
    try:
        joined_df = (
            items_df
            .join(orders_df.select("order_id", "order_date"), on="order_id")
            .join(products_df.select(col("id").alias("product_id"), "category"), on="product_id")
            .withColumn("is_returned", when(col("status") == "returned", 1).otherwise(0))
        )

        metrics_df = (
            joined_df.groupBy("category", "order_date")
            .agg(
                spark_round(_sum("sale_price"), 2).alias("daily_revenue"),
                spark_round(_sum("sale_price") / countDistinct("order_id"), 2).alias("avg_order_value"),
                spark_round(_sum("is_returned") / countDistinct("order_id"), 4).alias("avg_return_rate")
            )
        )
        
        logger.info("Category metrics calculated successfully")
        return metrics_df

    except Exception as e:
        logger.error(f"Failed to calculate category metrics: {e}")
        return None

def calculate_order_metrics(items_df, orders_df):
    """Calculate order-level metrics"""
    try:
        joined_df = (
            items_df
            .join(orders_df, "order_id")
            .select(
                "order_date",
                "order_id",
                "user_id",
                col("id").alias("item_id"),
                "sale_price",
                when(col("status") == "returned", 1).otherwise(0).alias("is_returned")
            )
        )

        metrics_df = (
            joined_df.groupBy("order_date")
            .agg(
                countDistinct("order_id").alias("total_orders"),
                spark_round(_sum("sale_price"), 2).alias("total_revenue"),
                count("item_id").alias("total_items_sold"),
                spark_round(_sum("is_returned") / countDistinct("order_id"), 4).alias("return_rate"),
                countDistinct("user_id").alias("unique_customers")
            )
        )
        
        logger.info("Order metrics calculated successfully")
        return metrics_df

    except Exception as e:
        logger.error(f"Failed to calculate order metrics: {e}")
        return None

def convert_to_decimal(value):
    """Convert numeric values to Decimal for DynamoDB"""
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    return value

def write_metrics_to_dynamodb(spark_df, table_name, primary_keys):
    """Generic function to write metrics to DynamoDB with retry logic"""
    logger.info(f"Writing metrics to DynamoDB table: {table_name}")
    
    # Dynamic partitioning based on data size
    row_count = spark_df.count()
    optimal_partitions = max(1, min(10, row_count // 1000))
    spark_df = spark_df.coalesce(optimal_partitions)

    def process_partition(iterator):
        import boto3
        from decimal import Decimal
        from botocore.exceptions import ClientError
        
        dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
        table = dynamodb.Table(table_name)
        
        batch_items = []
        batch_size = 25
        successful_items = 0
        failed_items = 0

        def write_batch(items):
            nonlocal successful_items, failed_items
            if not items:
                return
            
            max_retries = 3
            for attempt in range(max_retries + 1):
                try:
                    with table.batch_writer(overwrite_by_pkeys=primary_keys) as batch:
                        for item in items:
                            batch.put_item(Item=item)
                    successful_items += len(items)
                    return
                except ClientError as e:
                    if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException' and attempt < max_retries:
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    failed_items += len(items)
                    break
                except Exception:
                    failed_items += len(items)
                    break

        for row in iterator:
            try:
                item_dict = row.asDict(recursive=True)
                
                # Validate required fields
                if not all(key in item_dict for key in primary_keys):
                    continue
                
                # Convert numeric values to Decimal
                for key, value in item_dict.items():
                    if isinstance(value, (int, float)):
                        item_dict[key] = Decimal(str(value))
                    elif isinstance(value, (datetime.date, datetime.datetime)):
                        item_dict[key] = value.isoformat()
                
                batch_items.append(item_dict)
                
                if len(batch_items) >= batch_size:
                    write_batch(batch_items)
                    batch_items = []
                    
            except Exception:
                failed_items += 1

        if batch_items:
            write_batch(batch_items)

    try:
        spark_df.rdd.foreachPartition(process_partition)
        logger.info(f"Completed writing to DynamoDB table: {table_name}")
    except Exception as e:
        logger.error(f"Error writing to {table_name}: {e}")
        raise

def run_analytics_pipeline():
    """Main pipeline orchestration"""
    spark = None
    files_to_process = {}
    
    try:
        # Get files to process before starting
        files_to_process = get_s3_files_to_process()
        if not files_to_process:
            logger.warning("No files found to process")
            return
        
        # Setup infrastructure
        if not setup_dynamodb_tables():
            logger.error("Failed to setup DynamoDB tables")
            return
        
        spark = setup_spark_session()
        if not spark:
            return

        # Load and transform data
        orders_df, items_df, products_df = load_data_from_s3(spark)
        if not all([orders_df, items_df, products_df]):
            return

        orders_df, items_df = transform_data(orders_df, items_df)
        if not all([orders_df, items_df]):
            return

        # Calculate metrics
        category_metrics_df = calculate_category_metrics(items_df, orders_df, products_df)
        order_metrics_df = calculate_order_metrics(items_df, orders_df)

        # Write to DynamoDB
        if category_metrics_df:
            write_metrics_to_dynamodb(
                category_metrics_df, 
                "category_metrics_table", 
                ['category', 'order_date']
            )
        
        if order_metrics_df:
            write_metrics_to_dynamodb(
                order_metrics_df, 
                "order_metrics_table", 
                ['order_date']
            )

        # Archive processed files
        if not archive_processed_files(files_to_process):
            logger.warning("Failed to archive some files")
        
        logger.info("Analytics pipeline completed successfully")

    except Exception as e:
        logger.error(f"Analytics pipeline failed: {e}")
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session terminated")

if __name__ == "__main__":
    run_analytics_pipeline()