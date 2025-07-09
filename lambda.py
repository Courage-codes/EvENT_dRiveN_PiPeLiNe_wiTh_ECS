import json
import boto3
import time
from datetime import datetime
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    """
    Lambda function that checks for complete file sets and triggers Step Functions
    when all required files (orders, order_items, products) are present
    """
    
    # Initialize AWS clients
    s3_client = boto3.client('s3')
    stepfunctions = boto3.client('stepfunctions')
    
    # Configuration
    bucket_name = 'ecs.data'
    stepfunctions_arn = 'arn:aws:states:us-east-1:643303011741:stateMachine:ecsJob'
    
    # Required file folders
    required_folders = {
        'orders': 'orders/',
        'order_items': 'order_items/',
        'products': 'products/'
    }
    
    print(f"Lambda triggered by S3 event. Checking for complete file set...")
    
    # Check each required folder for CSV files
    all_files_present = True
    file_status = {}
    file_details = {}
    
    for folder_type, prefix in required_folders.items():
        has_files, files_found = check_folder_has_files(s3_client, bucket_name, prefix)
        file_status[folder_type] = has_files
        file_details[folder_type] = files_found
        
        if not has_files:
            all_files_present = False
    
    # Log current status
    print(f"File status check: {file_status}")
    for folder, files in file_details.items():
        if files:
            print(f"  {folder}: {files}")
        else:
            print(f"  {folder}: No CSV files found")
    
    # Decision logic
    if all_files_present:
        print("✅ All required files present - triggering Step Functions")
        success = trigger_step_functions(stepfunctions, stepfunctions_arn, bucket_name, file_details)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Step Functions triggered successfully',
                'fileStatus': file_status,
                'filesFound': file_details,
                'triggered': success
            })
        }
    else:
        print("⏳ Waiting for more files - Step Functions not triggered")
        missing_folders = [folder for folder, status in file_status.items() if not status]
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Waiting for complete file set',
                'fileStatus': file_status,
                'missingFolders': missing_folders,
                'triggered': False
            })
        }

def check_folder_has_files(s3_client, bucket, prefix):
    """
    Check if a folder contains any CSV files
    Returns: (has_files: bool, files_found: list)
    """
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=50  # Reasonable limit for file checking
        )
        
        if 'Contents' not in response:
            return False, []
        
        # Find CSV files (not directories)
        csv_files = []
        for obj in response['Contents']:
            key = obj['Key']
            # Check if it's a CSV file and not just a folder
            if key.endswith('.csv') and not key.endswith('/'):
                csv_files.append(key)
        
        return len(csv_files) > 0, csv_files
        
    except ClientError as e:
        print(f"Error checking folder {prefix}: {e}")
        return False, []
    except Exception as e:
        print(f"Unexpected error checking folder {prefix}: {e}")
        return False, []

def trigger_step_functions(stepfunctions, arn, bucket, file_details):
    """
    Trigger Step Functions execution with complete file set
    """
    try:
        # Create unique execution name
        timestamp = int(time.time())
        execution_name = f'auto-complete-set-{timestamp}'
        
        # Prepare input JSON for Step Functions
        input_json = {
            "fileKey": "",
            "bucket": bucket,
            "triggeredBy": "lambda-aggregator",
            "timestamp": datetime.now().isoformat(),
            "filesFound": file_details
        }
        
        # Start Step Functions execution
        response = stepfunctions.start_execution(
            stateMachineArn=arn,
            name=execution_name,
            input=json.dumps(input_json)
        )
        
        print(f"✅ Step Functions execution started successfully:")
        print(f"   Execution ARN: {response['executionArn']}")
        print(f"   Input JSON: {input_json}")
        
        return True
        
    except ClientError as e:
        print(f"❌ AWS error triggering Step Functions: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error triggering Step Functions: {e}")
        return False

def get_s3_event_info(event):
    """
    Extract S3 event information for logging
    """
    try:
        if 'Records' in event:
            for record in event['Records']:
                if 's3' in record:
                    bucket = record['s3']['bucket']['name']
                    key = record['s3']['object']['key']
                    event_name = record['eventName']
                    return bucket, key, event_name
        return None, None, None
    except Exception as e:
        print(f"Error parsing S3 event: {e}")
        return None, None, None

# Optional: Enhanced version with S3 event logging
def lambda_handler_with_event_logging(event, context):
    """
    Enhanced version that logs S3 event details
    """
    # Log the triggering event
    bucket, key, event_name = get_s3_event_info(event)
    if bucket and key:
        print(f"Triggered by S3 event: {event_name} for {bucket}/{key}")
    
    # Call main handler
    return lambda_handler(event, context)
