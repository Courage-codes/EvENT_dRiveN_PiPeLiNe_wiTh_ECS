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
        # Create a fresh boto3 session for table creation
        boto_session = boto3.Session(region_name=AWS_REGION)
        dynamodb = boto_session.resource('dynamodb')
        
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
        logger.info(f"Waiting for table {table_name} to be created...")
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

def setup_spark_with_s3():
    """Set up Spark session with S3A configuration using path-style access"""
    try:
        spark = SparkSession.builder \
            .appName("ECSAnalytics") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.region", AWS_REGION) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        logger.info("Initialized Spark session with S3A path-style configuration")
        return spark
    except Exception as e:
        logger.exception("Failed to initialize Spark session: %s", e)
        return None

def fetch_and_cache_s3_data(spark):
    """Fetch and cache data from S3"""
    try:
        orders_path = f"s3a://{S3_BUCKET}/{DATA_PATHS['orders']}"
        items_path = f"s3a://{S3_BUCKET}/{DATA_PATHS['order_items']}"
        products_path = f"s3a://{S3_BUCKET}/{DATA_PATHS['products']}"
        
        logger.info(f"Loading orders from: {orders_path}")
        orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv(orders_path)
        logger.info(f"Orders raw count: {orders_df.count()}")

        logger.info(f"Loading order items from: {items_path}")
        items_df = spark.read.option("header", "true").option("inferSchema", "true").csv(items_path)
        logger.info(f"Order items raw count: {items_df.count()}")

        logger.info(f"Loading products from: {products_path}")
        products_df = spark.read.option("header", "true").option("inferSchema", "true").csv(products_path)
        logger.info(f"Products raw count: {products_df.count()}")

        logger.info("Successfully loaded data from S3")
        return orders_df, items_df, products_df

    except Exception as e:
        logger.exception(f"Failed to load S3 data: {e}")
        return None, None, None

def clean_orders(orders_df):
    """
    Clean and validate orders data.
    Mandatory fields: order_id, user_id, created_at, status.
    Impute missing values for mandatory fields.
    """
    logger.info("Starting cleaning for orders data.")

    # Define imputation values for mandatory fields
    impute_values = {
        "order_id": "UNKNOWN_ORDER",
        "user_id": "UNKNOWN_USER",
        "created_at": "1970-01-01",  # Default date
        "status": "UNKNOWN_STATUS"
    }

    # Impute missing values for mandatory fields
    orders_df = orders_df.fillna(impute_values)

    # Validate and convert created_at to date
    orders_df = orders_df.withColumn("order_date", to_date(col("created_at")))
    orders_df = orders_df.filter(col("order_date").isNotNull())

    # Drop duplicate orders (if any)
    orders_df = orders_df.dropDuplicates(["order_id"])

    logger.info(f"Orders cleaned. Remaining records: {orders_df.count()}")
    return orders_df

def clean_order_items(order_items_df):
    """
    Clean and validate order_items data.
    Mandatory fields: id, order_id, product_id, sale_price.
    Impute missing values for mandatory fields.
    """
    logger.info("Starting cleaning for order_items data.")

    # Define imputation values for mandatory fields
    impute_values = {
        "id": "UNKNOWN_ID",
        "order_id": "UNKNOWN_ORDER",
        "product_id": "UNKNOWN_PRODUCT",
        "sale_price": 0.0  # Default price
    }

    # Impute missing values for mandatory fields
    order_items_df = order_items_df.fillna(impute_values)

    # Ensure sale_price is a valid float
    order_items_df = order_items_df.withColumn("sale_price", col("sale_price").cast("float"))
    order_items_df = order_items_df.filter(col("sale_price").isNotNull())

    # Drop duplicate order items if necessary
    order_items_df = order_items_df.dropDuplicates(["id"])

    logger.info(f"Order items cleaned. Remaining records: {order_items_df.count()}")
    return order_items_df

def clean_products(products_df):
    """
    Clean and validate products data.
    Mandatory fields: id, sku, cost, category, retail_price.
    Impute missing values for mandatory fields.
    """
    logger.info("Starting cleaning for products data.")

    # Define imputation values for mandatory fields
    impute_values = {
        "id": "UNKNOWN_ID",
        "sku": "UNKNOWN_SKU",
        "cost": 0.0,  # Default cost
        "category": "UNKNOWN_CATEGORY",
        "retail_price": 0.0  # Default price
    }

    # Impute missing values for mandatory fields
    products_df = products_df.fillna(impute_values)

    # Convert cost and retail_price to float
    products_df = products_df.withColumn("cost", col("cost").cast("float"))
    products_df = products_df.withColumn("retail_price", col("retail_price").cast("float"))
    products_df = products_df.filter(col("cost").isNotNull() & col("retail_price").isNotNull())

    # Drop duplicates
    products_df = products_df.dropDuplicates(["id"])

    logger.info(f"Products cleaned. Remaining records: {products_df.count()}")
    return products_df

def clean_and_transform_dataframes(orders_df, items_df, products_df):
    """Clean and transform all dataframes with comprehensive data validation"""
    try:
        logger.info("Starting comprehensive data cleaning and transformation...")
        
        # Clean each dataset using the comprehensive cleaning logic
        orders_clean = clean_orders(orders_df)
        order_items_clean = clean_order_items(items_df)
        products_clean = clean_products(products_df)
        
        # Cache cleaned dataframes for performance
        orders_clean.cache()
        order_items_clean.cache()
        products_clean.cache()
        
        # Validate that we have data after cleaning
        orders_count = orders_clean.count()
        items_count = order_items_clean.count()
        products_count = products_clean.count()
        
        if orders_count == 0:
            logger.error("No valid orders data after cleaning")
            return None, None, None
        if items_count == 0:
            logger.error("No valid order items data after cleaning")
            return None, None, None
        if products_count == 0:
            logger.error("No valid products data after cleaning")
            return None, None, None
        
        logger.info(f"Data cleaning completed successfully:")
        logger.info(f"  - Orders: {orders_count} records")
        logger.info(f"  - Order Items: {items_count} records")
        logger.info(f"  - Products: {products_count} records")
        
        return orders_clean, order_items_clean, products_clean
        
    except Exception as e:
        logger.exception("Data cleaning and transformation failed: %s", e)
        return None, None, None

def calculate_category_metrics(items_df, orders_df, products_df):
    """Calculate daily revenue, average order value, and return rate by category"""
    try:
        joined_df = (
            items_df
            .join(orders_df.select("order_id", "order_date", "status"), on="order_id")
            .join(products_df.select(col("id").alias("product_id"), "category"), on="product_id")
            .withColumn("is_returned", when(col("status") == "returned", 1).otherwise(0))
        )
        logger.info("Joined data for category metrics")

        metrics_df = (
            joined_df.groupBy("category", "order_date")
            .agg(
                spark_round(_sum("sale_price"), 2).alias("daily_revenue"),
                spark_round(_sum("sale_price") / countDistinct("order_id"), 2).alias("avg_order_value"),
                spark_round(_sum("is_returned") / countDistinct("order_id"), 4).alias("avg_return_rate")
            ).cache()
        )
        logger.info("Category metrics calculated")
        metrics_df.show(5, truncate=False)
        return metrics_df

    except Exception as e:
        logger.exception("Failed to calculate category metrics: %s", e)
        return None

def calculate_order_metrics(items_df, orders_df):
    """Calculate order-level metrics including total orders, revenue, items sold, return rate, and unique customers"""
    try:
        joined_df = (
            items_df.alias("oi")
            .join(orders_df.alias("o"), col("oi.order_id") == col("o.order_id"))
            .select(
                col("o.order_date"),
                col("o.order_id"),
                col("o.user_id"),
                col("oi.id").alias("item_id"),
                col("oi.sale_price"),
                when(col("o.status") == "returned", 1).otherwise(0).alias("is_returned")
            )
        )
        logger.info("Joined data for order metrics")

        metrics_df = (
            joined_df.groupBy("order_date")
            .agg(
                countDistinct("order_id").alias("total_orders"),
                spark_round(_sum("sale_price"), 2).alias("total_revenue"),
                count("item_id").alias("total_items_sold"),
                spark_round(_sum("is_returned") / countDistinct("order_id"), 4).alias("return_rate"),
                countDistinct("user_id").alias("unique_customers")
            ).cache()
        )
        logger.info("Order metrics calculated")
        metrics_df.show(5, truncate=False)
        return metrics_df

    except Exception as e:
        logger.exception("Failed to calculate order metrics: %s", e)
        return None

def write_category_metrics_to_dynamodb(spark_df, table_name):
    """Write category metrics to DynamoDB with batch operations and retry logic"""
    logger.info(f"Writing category metrics to DynamoDB table: {table_name}")
    
    spark_df = spark_df.coalesce(5)
    logger.info("Coalesced DataFrame to 5 partitions")

    def process_partition(iterator):
        # Create a new boto3 session inside the partition to avoid serialization issues
        import boto3
        import json
        import time
        import datetime
        from decimal import Decimal
        from botocore.exceptions import ClientError
        
        # Create fresh boto3 session in each partition
        local_session = boto3.Session(region_name=AWS_REGION)
        dynamodb = local_session.resource('dynamodb')
        table = dynamodb.Table(table_name)
        
        class DateEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, (datetime.date, datetime.datetime)):
                    return obj.isoformat()
                return super().default(obj)

        items_processed = 0
        successful_items = 0
        failed_items = 0
        batch_items = []
        batch_size = 25

        def write_batch(items):
            nonlocal successful_items, failed_items
            if not items:
                return
            max_retries = 3
            base_delay = 1
            for attempt in range(max_retries + 1):
                try:
                    with table.batch_writer(overwrite_by_pkeys=['category', 'order_date']) as batch:
                        for item in items:
                            batch.put_item(Item=item)
                    successful_items += len(items)
                    print(f"Wrote batch of {len(items)} items (attempt {attempt + 1})")
                    return
                except ClientError as e:
                    error_code = e.response['Error']['Code']
                    if error_code == 'ProvisionedThroughputExceededException' and attempt < max_retries:
                        delay = base_delay * (2 ** attempt)
                        print(f"Throughput exceeded, retrying in {delay}s (attempt {attempt + 1})")
                        time.sleep(delay)
                        continue
                    print(f"ClientError in batch write (attempt {attempt + 1}): {e}")
                    if attempt == max_retries:
                        failed_items += len(items)
                        break
                except Exception as e:
                    print(f"Unexpected error in batch write (attempt {attempt + 1}): {e}")
                    if attempt == max_retries:
                        failed_items += len(items)
                        break

        for row in iterator:
            items_processed += 1
            try:
                item_dict_raw = row.asDict(recursive=True)
                if 'category' not in item_dict_raw or 'order_date' not in item_dict_raw:
                    print(f"Skipping row missing required fields: {item_dict_raw}")
                    continue
                item_json = json.dumps(item_dict_raw, cls=DateEncoder)
                item_dict = json.loads(item_json, parse_float=Decimal)
                batch_items.append(item_dict)
                if len(batch_items) >= batch_size:
                    write_batch(batch_items)
                    batch_items = []
            except Exception as e:
                print(f"Error processing row: {e}")
                failed_items += 1

        if batch_items:
            write_batch(batch_items)

        print(f"Partition processed: {items_processed} items, {successful_items} successful, {failed_items} failed")

    try:
        spark_df.rdd.foreachPartition(process_partition)
        logger.info(f"Completed writing to DynamoDB table: {table_name}")
    except Exception as e:
        logger.error(f"Error writing to {table_name}: {e}", exc_info=True)
        raise

def write_order_metrics_to_dynamodb(spark_df, table_name):
    """Write order metrics to DynamoDB with batch operations and retry logic"""
    logger.info(f"Writing order metrics to DynamoDB table: {table_name}")
    
    spark_df = spark_df.coalesce(5)
    logger.info("Coalesced DataFrame to 5 partitions")

    def process_partition(iterator):
        # Create a new boto3 session inside the partition to avoid serialization issues
        import boto3
        import json
        import time
        import datetime
        from decimal import Decimal
        from botocore.exceptions import ClientError
        
        # Create fresh boto3 session in each partition
        local_session = boto3.Session(region_name=AWS_REGION)
        dynamodb = local_session.resource('dynamodb')
        table = dynamodb.Table(table_name)
        
        class DateEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, (datetime.date, datetime.datetime)):
                    return obj.isoformat()
                return super().default(obj)

        items_processed = 0
        successful_items = 0
        failed_items = 0
        batch_items = []
        batch_size = 25

        def write_batch(items):
            nonlocal successful_items, failed_items
            if not items:
                return
            max_retries = 3
            base_delay = 1
            for attempt in range(max_retries + 1):
                try:
                    with table.batch_writer(overwrite_by_pkeys=['order_date']) as batch:
                        for item in items:
                            batch.put_item(Item=item)
                    successful_items += len(items)
                    print(f"Wrote batch of {len(items)} items (attempt {attempt + 1})")
                    return
                except ClientError as e:
                    error_code = e.response['Error']['Code']
                    if error_code == 'ProvisionedThroughputExceededException' and attempt < max_retries:
                        delay = base_delay * (2 ** attempt)
                        print(f"Throughput exceeded, retrying in {delay}s (attempt {attempt + 1})")
                        time.sleep(delay)
                        continue
                    print(f"ClientError in batch write (attempt {attempt + 1}): {e}")
                    if attempt == max_retries:
                        failed_items += len(items)
                        break
                except Exception as e:
                    print(f"Unexpected error in batch write (attempt {attempt + 1}): {e}")
                    if attempt == max_retries:
                        failed_items += len(items)
                        break

        for row in iterator:
            items_processed += 1
            try:
                item_dict_raw = row.asDict(recursive=True)
                if 'order_date' not in item_dict_raw:
                    print(f"Skipping row missing order_date: {item_dict_raw}")
                    continue
                item_json = json.dumps(item_dict_raw, cls=DateEncoder)
                item_dict = json.loads(item_json, parse_float=Decimal)
                batch_items.append(item_dict)
                if len(batch_items) >= batch_size:
                    write_batch(batch_items)
                    batch_items = []
            except Exception as e:
                print(f"Error processing row: {e}")
                failed_items += 1

        if batch_items:
            write_batch(batch_items)

        print(f"Partition processed: {items_processed} items, {successful_items} successful, {failed_items} failed")

    try:
        spark_df.rdd.foreachPartition(process_partition)
        logger.info(f"Completed writing to DynamoDB table: {table_name}")
    except Exception as e:
        logger.error(f"Error writing to {table_name}: {e}", exc_info=True)
        raise

def release_dataframe_cache(orders_df, items_df, products_df, category_metrics_df, order_metrics_df):
    """Release cached DataFrames to free memory"""
    try:
        if order_metrics_df is not None:
            order_metrics_df.unpersist()
        if category_metrics_df is not None:
            category_metrics_df.unpersist()
        
        orders_df.unpersist()
        items_df.unpersist()
        products_df.unpersist()
        
        logger.info("Successfully released cached DataFrames")
    except Exception as e:
        logger.warning(f"Failed to release cached DataFrames: {e}")

def run_analytics_pipeline():
    """Orchestrate the analytics pipeline with comprehensive data cleaning"""
    spark = None
    try:
        # Setup DynamoDB tables first
        if not setup_dynamodb_tables():
            logger.error("Failed to setup DynamoDB tables, exiting")
            return
        
        spark = setup_spark_with_s3()
        if spark is None:
            return

        # Fetch raw data from S3
        orders_df, items_df, products_df = fetch_and_cache_s3_data(spark)
        if orders_df is None or items_df is None or products_df is None:
            if spark:
                spark.stop()
            return

        # Clean and transform data with comprehensive validation
        orders_clean, items_clean, products_clean = clean_and_transform_dataframes(orders_df, items_df, products_df)
        if orders_clean is None or items_clean is None or products_clean is None:
            logger.error("Data cleaning failed, exiting pipeline")
            if spark:
                spark.stop()
            return

        # Calculate metrics using cleaned data
        category_metrics_df = calculate_category_metrics(items_clean, orders_clean, products_clean)
        order_metrics_df = calculate_order_metrics(items_clean, orders_clean)

        # Write metrics to DynamoDB
        try:
            if category_metrics_df is not None:
                write_category_metrics_to_dynamodb(category_metrics_df, table_name="category_metrics_table")
            else:
                logger.warning("Category metrics DataFrame is None, skipping DynamoDB write")
            
            if order_metrics_df is not None:
                write_order_metrics_to_dynamodb(order_metrics_df, table_name="order_metrics_table")
            else:
                logger.warning("Order metrics DataFrame is None, skipping DynamoDB write")
        except Exception as e:
            logger.exception("Failed to write metrics to DynamoDB: %s", e)

        # Clean up cached dataframes
        release_dataframe_cache(orders_clean, items_clean, products_clean, category_metrics_df, order_metrics_df)

    except Exception as e:
        logger.exception("Analytics pipeline failed: %s", e)
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session terminated")
        logger.info("Analytics pipeline completed")

if __name__ == "__main__":
    run_analytics_pipeline()