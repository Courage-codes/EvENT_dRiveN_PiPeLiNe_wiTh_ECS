import logging
import json
import time
import os
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

def send_task_success(task_token, output_data):
    """Send success signal to Step Functions"""
    try:
        stepfunctions = boto3.client('stepfunctions', region_name=AWS_REGION)
        stepfunctions.send_task_success(
            taskToken=task_token,
            output=json.dumps(output_data)
        )
        logger.info("Task success sent to Step Functions")
    except Exception as e:
        logger.error(f"Failed to send task success: {e}")
        raise

def send_task_failure(task_token, error_message):
    """Send failure signal to Step Functions"""
    try:
        stepfunctions = boto3.client('stepfunctions', region_name=AWS_REGION)
        stepfunctions.send_task_failure(
            taskToken=task_token,
            error="TransformationError",
            cause=error_message
        )
        logger.error(f"Task failure sent to Step Functions: {error_message}")
    except Exception as e:
        logger.error(f"Failed to send task failure: {e}")

def get_environment_variables():
    """Get required environment variables from ECS task"""
    required_vars = ['TASK_TOKEN', 'EXECUTION_ID', 'BUCKET_NAME']
    env_vars = {}
    
    for var in required_vars:
        value = os.environ.get(var)
        if not value:
            raise ValueError(f"Required environment variable {var} not found")
        env_vars[var] = value
    
    # FILE_KEY is optional - default to empty string for multi-file processing
    env_vars['FILE_KEY'] = os.environ.get('FILE_KEY', '')
    
    # Optional validation metadata
    validation_metadata = os.environ.get('VALIDATION_METADATA')
    if validation_metadata:
        try:
            env_vars['VALIDATION_METADATA'] = json.loads(validation_metadata)
        except json.JSONDecodeError:
            logger.warning("Invalid validation metadata JSON, ignoring")
            env_vars['VALIDATION_METADATA'] = None
    
    return env_vars

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

def get_files_from_folder(bucket_name, folder_path):
    """Get files from specific folder that was validated"""
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    files_to_process = {}
    
    try:
        # Handle empty folder_path for root-level processing
        base_prefix = folder_path if folder_path and not folder_path.endswith('/') else folder_path
        if base_prefix and not base_prefix.endswith('/'):
            base_prefix += '/'
        
        for data_type, path in DATA_PATHS.items():
            full_path = base_prefix + path
            logger.info(f"Looking for {data_type} files in: {full_path}")
            
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=full_path
            )
            
            if 'Contents' in response:
                files_to_process[data_type] = [obj['Key'] for obj in response['Contents'] 
                                             if obj['Key'].endswith('.csv')]
                logger.info(f"Found {len(files_to_process[data_type])} {data_type} files")
            else:
                files_to_process[data_type] = []
                logger.warning(f"No {data_type} files found in {full_path}")
        
        return files_to_process
    except Exception as e:
        logger.error(f"Failed to list S3 files: {e}")
        return {}

def archive_processed_files(files_to_process, bucket_name, execution_id):
    """Move processed files to archive bucket"""
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    
    moved_files = []
    failed_files = []
    
    try:
        for data_type, file_list in files_to_process.items():
            for file_key in file_list:
                try:
                    # Create archive key with execution ID and timestamp
                    archive_key = f"processed/{execution_id}/{timestamp}/{file_key}"
                    
                    # Copy file to archive bucket
                    copy_source = {'Bucket': bucket_name, 'Key': file_key}
                    s3_client.copy_object(
                        CopySource=copy_source,
                        Bucket=S3_ARCHIVE_BUCKET,
                        Key=archive_key
                    )
                    
                    # Verify the copy succeeded
                    try:
                        s3_client.head_object(Bucket=S3_ARCHIVE_BUCKET, Key=archive_key)
                        
                        # Delete from original bucket only after successful copy verification
                        s3_client.delete_object(Bucket=bucket_name, Key=file_key)
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
            return len(failed_files) == 0
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to archive files: {e}")
        return False

def load_data_from_s3(spark, bucket_name, files_to_process):
    """Load data from S3 using specific files"""
    try:
        logger.info("Loading data from S3...")
        
        # Check if we have the required files
        required_types = ['orders', 'order_items', 'products']
        for req_type in required_types:
            if not files_to_process.get(req_type):
                raise ValueError(f"No {req_type} files found for processing")
        
        # Build S3 paths for each data type
        orders_paths = [f"s3a://{bucket_name}/{file}" for file in files_to_process['orders']]
        items_paths = [f"s3a://{bucket_name}/{file}" for file in files_to_process['order_items']]
        products_paths = [f"s3a://{bucket_name}/{file}" for file in files_to_process['products']]
        
        # Load data with union for multiple files - cache orders and items as they're used multiple times
        orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv(orders_paths).cache()
        items_df = spark.read.option("header", "true").option("inferSchema", "true").csv(items_paths).cache()
        products_df = spark.read.option("header", "true").option("inferSchema", "true").csv(products_paths)

        # Validate data loaded
        orders_count = orders_df.count()
        items_count = items_df.count()
        products_count = products_df.count()
        
        logger.info(f"Data loaded successfully - Orders: {orders_count}, Items: {items_count}, Products: {products_count}")
        
        if orders_count == 0 or items_count == 0 or products_count == 0:
            raise ValueError("One or more datasets are empty")
        
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
    """Calculate order-level metrics with explicit column selection to avoid ambiguity"""
    try:
        # Explicitly select columns from each DataFrame to avoid ambiguous references
        items_selected = items_df.select(
            "order_id",
            col("id").alias("item_id"),
            "sale_price",
            when(col("status") == "returned", 1).otherwise(0).alias("is_returned")
        )
        
        orders_selected = orders_df.select(
            "order_id",
            "order_date", 
            "user_id"  # Only select user_id from orders table
        )
        
        # Join the pre-selected DataFrames
        joined_df = items_selected.join(orders_selected, "order_id")

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
                        time.sleep(2 ** attempt)
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

        return successful_items, failed_items

    try:
        # Collect results from all partitions
        results = spark_df.rdd.mapPartitions(process_partition).collect()
        total_successful = sum(result[0] for result in results if result)
        total_failed = sum(result[1] for result in results if result)
        
        logger.info(f"DynamoDB write completed for {table_name}: {total_successful} successful, {total_failed} failed")
        return total_successful, total_failed
        
    except Exception as e:
        logger.error(f"Error writing to {table_name}: {e}")
        raise

def run_transformation_task():
    """Main transformation task for ECS"""
    spark = None
    task_token = None
    
    try:
        # Get environment variables
        env_vars = get_environment_variables()
        task_token = env_vars['TASK_TOKEN']
        file_key = env_vars['FILE_KEY']
        execution_id = env_vars['EXECUTION_ID']
        bucket_name = env_vars['BUCKET_NAME']
        validation_metadata = env_vars.get('VALIDATION_METADATA')
        
        logger.info(f"Starting transformation for execution: {execution_id}, folder: {file_key}")
        
        # Setup infrastructure
        if not setup_dynamodb_tables():
            raise Exception("Failed to setup DynamoDB tables")
        
        spark = setup_spark_session()
        if not spark:
            raise Exception("Failed to initialize Spark session")

        # Get files from the specific folder that was validated
        files_to_process = get_files_from_folder(bucket_name, file_key)
        if not files_to_process:
            raise Exception("No files found to process")

        # Load and transform data
        orders_df, items_df, products_df = load_data_from_s3(spark, bucket_name, files_to_process)
        if not all([orders_df, items_df, products_df]):
            raise Exception("Failed to load data from S3")

        orders_df, items_df = transform_data(orders_df, items_df)
        if not all([orders_df, items_df]):
            raise Exception("Failed to transform data")

        # Calculate metrics with the fixed functions
        category_metrics_df = calculate_category_metrics(items_df, orders_df, products_df)
        order_metrics_df = calculate_order_metrics(items_df, orders_df)

        if not category_metrics_df or not order_metrics_df:
            raise Exception("Failed to calculate metrics")

        # Write to DynamoDB and collect statistics
        total_records_processed = 0
        
        cat_success, cat_failed = write_metrics_to_dynamodb(
            category_metrics_df, 
            "category_metrics_table", 
            ['category', 'order_date']
        )
        total_records_processed += cat_success
        
        ord_success, ord_failed = write_metrics_to_dynamodb(
            order_metrics_df, 
            "order_metrics_table", 
            ['order_date']
        )
        total_records_processed += ord_success

        # Archive processed files
        archive_success = archive_processed_files(files_to_process, bucket_name, execution_id)
        
        # Prepare success response
        success_response = {
            "status": "success",
            "recordsProcessed": total_records_processed,
            "categoryMetrics": cat_success,
            "orderMetrics": ord_success,
            "failedRecords": cat_failed + ord_failed,
            "archiveStatus": "success" if archive_success else "partial_failure",
            "executionId": execution_id,
            "processedFiles": {
                "orders": len(files_to_process.get('orders', [])),
                "orderItems": len(files_to_process.get('order_items', [])),
                "products": len(files_to_process.get('products', []))
            }
        }
        
        # Send success to Step Functions
        send_task_success(task_token, success_response)
        logger.info(f"Transformation completed successfully for execution: {execution_id}")

    except Exception as e:
        error_message = f"Transformation failed: {str(e)}"
        logger.error(error_message)
        
        if task_token:
            send_task_failure(task_token, error_message)
        
        raise e
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session terminated")

if __name__ == "__main__":
    run_transformation_task()
