import boto3
import json

sfn_client = boto3.client("stepfunctions")

def lambda_handler(event, context):
    # Extract S3 bucket & file name
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_key = event["Records"][0]["s3"]["object"]["key"]

    print(f"New file detected: s3://{bucket_name}/{file_key}")

    # Start Step Function execution
    response = sfn_client.start_execution(
        stateMachineArn="arn:aws:states:us-east-1:123456789012:stateMachine:MySCDStepFunction",
        input=json.dumps({"s3_file": f"s3://{bucket_name}/{file_key}"})
    )

    return {
        "statusCode": 200,
        "body": json.dumps("Step Function triggered successfully!")
    }
