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

def fetch_and_cache_s3_data(spark):
    """Fetch and cache data from S3"""
    try:
        orders_path = f"s3a://{S3_BUCKET}/{DATA_PATHS['orders']}"
        items_path = f"s3a://{S3_BUCKET}/{DATA_PATHS['order_items']}"
        products_path = f"s3a://{S3_BUCKET}/{DATA_PATHS['products']}"
        
        logger.info(f"Loading orders from: {orders_path}")
        orders_df = spark.read.parquet(orders_path).cache()

        logger.info(f"Loading order items from: {items_path}")
        items_df = spark.read.parquet(items_path).cache()

        logger.info(f"Loading products from: {products_path}")
        products_df = spark.read.parquet(products_path).cache()

        logger.info("Successfully loaded data from S3")
        return orders_df, items_df, products_df

    except Exception as e:
        logger.exception(f"Failed to load S3 data: {e}")
        return None, None, None

def transform_dataframes(orders_df, items_df):
    """Transform data by adding date columns and casting types"""
    try:
        orders_df = orders_df.withColumn("order_date", to_date(col("created_at")))
        items_df = items_df.withColumn("sale_price", col("sale_price").cast("float"))
        logger.info("Data transformation completed")
        return orders_df, items_df
    except Exception as e:
        logger.exception("Data transformation failed: %s", e)
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
