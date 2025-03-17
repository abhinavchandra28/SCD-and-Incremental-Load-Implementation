
import json
import boto3

glue_client = boto3.client("glue")

def lambda_handler(event, context):
    print("Event received:", json.dumps(event, indent=2))
    
    # Extract bucket & file name from event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_name = event['Records'][0]['s3']['object']['key']

    print(f"New file detected: {file_name} in {bucket_name}")

    # Trigger Glue Job
    response = glue_client.start_job_run(JobName='SCD_ETL_Job')

    print("Glue ETL Job triggered successfully:", response['JobRunId'])
    return {"statusCode": 200, "body": "Glue Job Started"}
