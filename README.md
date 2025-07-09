# E-commerce Data Processing Pipeline - Comprehensive Documentation

## Table of Contents

- Project Overview
- Architecture
- Components
- Setup and Deployment
- Configuration
- Usage
- Testing
- Monitoring and Troubleshooting
- File Structure
- API Reference

## Project Overview

### Description

The E-commerce Pipeline is an event-driven, serverless data processing workflow on AWS designed to validate and transform e-commerce transactional data (orders, order items, and products), compute key performance indicators (KPIs), and store results in DynamoDB. The pipeline is triggered by S3 events, processes data through a series of validation and transformation steps using ECS Fargate tasks, archives data in S3, and sends notifications via SNS. The entire workflow is orchestrated using AWS Step Functions, ensuring reliability, error handling, and monitoring.

### Objectives

- **Validate Data**: Ensure incoming e-commerce data (products, orders, order items) meets predefined schema criteria.
- **Transform Data**: Compute category-level and order-level KPIs from validated data using Apache Spark.
- **Store Results**: Save computed KPIs to DynamoDB for downstream analytics.
- **Archive Data**: Move processed or invalid data to appropriate S3 prefixes for archival.
- **Notify Stakeholders**: Send success or failure notifications via SNS.
### Key Features

- **Automated File Processing**: Triggers when all required files (orders, order_items, products) are uploaded to S3
- **Concurrency Control**: Prevents overlapping executions using DynamoDB locks
- **Data Validation**: Schema validation and file quarantine for invalid data
- **Spark-based Transformation**: Scalable data processing with Apache Spark
- **Metrics Generation**: Category and order-level analytics stored in DynamoDB
- **File Archival**: Automatic archival of processed files
- **CI/CD Pipeline**: Automated testing and deployment via GitHub Actions

### Technology Stack

- AWS Step Functions: Workflow orchestration
- AWS ECS Fargate: Containerized data processing
- Apache Spark: Large-scale data transformation
- Amazon DynamoDB: Metrics storage and concurrency control
- Amazon S3: Data storage and archival
- Amazon ECR: Container image registry
- AWS Lambda: File aggregation logic
- Amazon SNS: Notifications
- GitHub Actions: CI/CD automation

## Architecture

### High-Level Architecture
![](images/architecture_diagram.svg)
### Step Function Architecture
![](images/step_function.png)
### Data Flow

1. **File Upload**: CSV files uploaded to S3 bucket 
2. **File Aggregation**: Lambda function checks for complete file set
3. **Workflow Trigger**: Step Functions execution starts when all files present
4. **Concurrency Control**: DynamoDB lock acquired to prevent overlapping processing
5. **Validation**: ECS task validates file schemas and quarantines invalid files
6. **Transformation**: ECS task processes data using Spark and calculates metrics
7. **Storage**: Metrics stored in DynamoDB tables
8. **Archival**: Processed files moved to archive bucket
9. **Notification**: SNS alerts sent on success/failure

### Concurrency Control

The pipeline implements a distributed semaphore pattern using DynamoDB to ensure only one execution processes files at a time:

- Lock acquisition before processing
- Automatic retry mechanism for waiting executions
- Lock release on completion or failure
- Prevents data corruption and duplicate processing

## Components

### 1. Step Functions State Machine

**Purpose**: Orchestrates the entire data processing workflow

**States**:

- AcquireLock: Obtains processing lock from DynamoDB
- ValidateFile: Runs validation ECS task
- CheckValidationResult: Routes based on validation outcome
- TransformData: Runs transformation ECS task
- ReleaseLockAndNotifySuccess: Releases lock and sends success notification
- ReleaseLockAndNotifyFailure: Releases lock and sends failure notification

**Configuration**:

```json
{
  "Comment": "Enhanced E-commerce Pipeline - ECS + Concurrency Control + Essential SNS Notifications",
  "StartAt": "AcquireLock"
}
```

### 2. Lambda File Aggregator

**Purpose**: Monitors S3 uploads and triggers Step Functions when complete dataset is available

**Functionality**:

- Checks for files in all three required folders (orders/, order_items/, products/)
- Only triggers Step Functions when all file types are present
- Provides detailed logging of file status

**Trigger**: S3 events for uploads to specific prefixes

### 3. Validation Container (ECS Task)

**Purpose**: Validates CSV file schemas and quarantines invalid files

**Schema Requirements**:

- Orders: order_id, user_id, created_at, status
- Order Items: id, order_id, product_id, sale_price
- Products: id, sku, cost, category, retail_price

**Outputs**:

- Valid files remain in processing folders
- Invalid files moved to quarantine folders
- Validation results passed to transformation step

### 4. Transformation Container (ECS Task)

**Purpose**: Processes data using Apache Spark and calculates business metrics

**Data Processing**:

- Loads CSV files from S3 using Spark
- Performs data type conversions and joins
- Calculates category and order-level metrics
- Writes results to DynamoDB tables

**Metrics Calculated**:

- **Category Metrics**: Daily revenue, average order value, return rate by category
- **Order Metrics**: Total orders, revenue, items sold, return rate, unique customers

### 5. DynamoDB Tables

- **ProcessingLockTable**:
  - **Purpose**: Concurrency control
  - **Key**: LockId (String)
  - **Attributes**: currentlockcount, execution IDs with timestamps
- **category_metrics_table**:
  - **Purpose**: Category-level analytics
  - **Keys**: category (Hash), order_date (Range)
  - **Metrics**: daily_revenue, avg_order_value, avg_return_rate
- **order_metrics_table**:
  - **Purpose**: Order-level analytics
  - **Key**: order_date (Hash)
  - **Metrics**: total_orders, total_revenue, total_items_sold, return_rate, unique_customers

## Setup and Deployment

### Prerequisites

- AWS Account with appropriate permissions
- GitHub repository
- Docker installed locally
- AWS CLI configured

### Initial Setup

1. **Create S3 Buckets**

   ```bash
   aws s3 mb s3://bucket-name --region your-region
   aws s3 mb s3://bucket-name --region your-region
   ```
2. **Create ECR Repositories**

   ```bash
   aws ecr create-repository --repository-name validation/ecr --region your-region
   aws ecr create-repository --repository-name transformation/ecr --region your-region
   ```
3. **Create ECS Cluster**

   ```bash
   aws ecs create-cluster --cluster-name ecr-dynamo-project --region us-east-1
   ```
4. **Create DynamoDB Lock Table**

   ```bash
   aws dynamodb create-table \
     --table-name ProcessingLockTable \
     --attribute-definitions AttributeName=LockId,AttributeType=S \
     --key-schema AttributeName=LockId,KeyType=HASH \
     --billing-mode PAY_PER_REQUEST \
     --region us-east-1
   
   # Initialize lock record
   aws dynamodb put-item \
     --table-name ProcessingLockTable \
     --item '{"LockId": {"S": "ecommerce-pipeline-lock"}, "currentlockcount": {"N": "0"}}' \
     --region us-east-1
   ```
5. **Create SNS Topic**

   ```bash
   aws sns create-topic --name ecommerce-etl-alerts --region us-east-1
   ```

### GitHub Secrets Configuration

Set up the following secrets in your GitHub repository:

| Secret Name | Description |
| --- | --- |
| AWS_ACCESS_KEY_ID | AWS access key for deployment |
| AWS_SECRET_ACCESS_KEY | AWS secret key for deployment |
| AWS_ACCOUNT_ID | Your AWS account ID |

### Automated Deployment

The CI/CD pipeline automatically deploys changes when pushed to the main branch:

- **Tests**: Runs validation and transformation tests
- **Step Functions**: Updates state machine definition
- **ECR Images**: Builds and pushes container images
- **Notifications**: Provides deployment summary

## Configuration

### Environment Variables

**Validation Container**:

- TASK_TOKEN: Step Functions callback token
- FILE_KEY: Input file path (empty for multi-file processing)
- EXECUTION_ID: Step Functions execution name
- BUCKET_NAME: S3 bucket name
- AWS_REGION: AWS region
- S3_ARCHIVE_BUCKET: Archive bucket name
- LOG_LEVEL: Logging level

**Transformation Container**:

- All validation variables plus:
- VALIDATION_METADATA: Results from validation step
- S3_BUCKET: Data bucket name
- ORDERS_PATH: Orders folder path
- ORDER_ITEMS_PATH: Order items folder path
- PRODUCTS_PATH: Products folder path
- CATEGORY_METRICS_TABLE: Category metrics table name
- ORDER_METRICS_TABLE: Order metrics table name
- SPARK_APP_NAME: Spark application name
- DYNAMODB_BATCH_SIZE: Batch size for DynamoDB writes
- DYNAMODB_MAX_RETRIES: Maximum retry attempts

### File Structure Requirements

```
s3://your-bucket/
├── orders/
│   └── orders_part1.csv
├── order_items/
│   └── order_items_part1.csv
└── products/
    └── products.csv
```

## Usage

### Manual Execution

Trigger the pipeline manually using AWS CLI:

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:643303011741:stateMachine:ecsJob \
  --name manual-execution-$(date +%s) \
  --input '{
    "fileKey": "",
    "bucket": "ecs.data"
  }'
```

### Automatic Execution

The pipeline automatically triggers when:

- Files are uploaded to any of the required S3 folders
- Lambda function detects complete file set
- Step Functions execution starts with concurrency control

### Data Upload Process

1. **Upload Files**: Place CSV files in appropriate S3 folders
2. **Wait for Processing**: Lambda checks for complete set
3. **Monitor Execution**: Check Step Functions console for progress
4. **Review Results**: Check DynamoDB tables for metrics
5. **Verify Archive**: Confirm files moved to archive bucket

## Monitoring and Troubleshooting

### Monitoring Tools

- **Step Functions Console**:
  - Execution status and history
  - State-by-state execution details
  - Error messages and stack traces
- **CloudWatch Logs**:
  - ECS task logs for validation and transformation
  - Lambda function logs for file aggregation
  - Detailed error information
- **DynamoDB Console**:
  - Lock table status
  - Metrics data verification
  - Query results for analysis

### Common Issues and Solutions

**Lock Acquisition Failures**

- **Symptom**: ConditionalCheckFailedException in Step Functions
- **Causes**:
  - Missing DynamoDB lock table
  - Uninitialized lock record
  - Stuck lock from failed execution
- **Solutions**:

  ```bash
  # Check lock table exists
  aws dynamodb describe-table --table-name ProcessingLockTable
  
  # Reset stuck lock
  aws dynamodb put-item \
    --table-name ProcessingLockTable \
    --item '{"LockId": {"S": "ecommerce-pipeline-lock"}, "currentlockcount": {"N": "0"}}'
  ```

**File Processing Errors**

- **Symptom**: Validation or transformation failures
- **Causes**:
  - Invalid file schemas
  - Missing required files
  - Data type conversion errors
- **Solutions**:
  - Check CloudWatch logs for specific error messages
  - Verify file schemas match requirements
  - Ensure all three file types are present

**Container Image Issues**

- **Symptom**: ECS task failures
- **Causes**:
  - Image not found in ECR
  - Incorrect task definition
  - Resource constraints
- **Solutions**:
  - Verify ECR repository and image tags
  - Check ECS task definition configuration
  - Monitor resource utilization

### Performance Optimization

- **Spark Configuration**:
  - Adjust partition sizes based on data volume
  - Optimize memory allocation for containers
  - Use appropriate instance types for ECS tasks
- **DynamoDB Optimization**:
  - Monitor read/write capacity usage
  - Implement batch writing for large datasets
  - Use appropriate partition keys for even distribution

## File Structure

```
project-root/
├── .github/
│   └── workflows/
│       ├── ci-cd.yml                    # CI/CD pipeline
│       └── requirements.txt             # Pipeline dependencies
├── tests/
│   ├── test_validation.py               # Validation tests
│   └── test_transformation.py           # Transformation tests
├── validation-image/
│   ├── Dockerfile                       # Validation container
│   └── validation.py                    # Validation script
├── transformation-image/
│   ├── Dockerfile                       # Transformation container
│   └── transformation.py                # Transformation script
├── lambda_function.py                    # File aggregation logic
├── stepfunction.json                    # Step Functions definition
└── requirements-test.txt                # Test dependencies
```