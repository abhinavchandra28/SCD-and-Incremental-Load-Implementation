import boto3
import json

s3_client = boto3.client("s3")
iam_client = boto3.client("iam")
glue_client = boto3.client("glue")
lambda_client = boto3.client("lambda")
stepfunctions_client = boto3.client("stepfunctions")

bucket_name = "your-etl-bucket"

# Create S3 Bucket
s3_client.create_bucket(Bucket=bucket_name)
print(f"S3 Bucket {bucket_name} created successfully!")

# Upload Glue Script to S3
s3_client.upload_file("scripts/glue_etl.py", bucket_name, "glue_scripts/glue_etl.py")
print("Glue ETL script uploaded to S3!")

# Create IAM Role for Glue & Lambda
role_name = "GlueLambdaRole"
assume_role_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": ["glue.amazonaws.com", "lambda.amazonaws.com"]},
            "Action": "sts:AssumeRole"
        }
    ]
}

role = iam_client.create_role(
    RoleName=role_name,
    AssumeRolePolicyDocument=json.dumps(assume_role_policy)
)

print(f"IAM Role {role_name} created successfully!")

# Deploy Glue Job
response = glue_client.create_job(
    Name="SCD_ETL_Job",
    Role=role_name,
    Command={"Name": "glueetl", "ScriptLocation": f"s3://{bucket_name}/glue_scripts/glue_etl.py"},
    GlueVersion="3.0",
)
print("Glue ETL Job created successfully!")

# Deploy Lambda Function
with open("scripts/lambda_trigger.zip", "rb") as f:
    lambda_code = f.read()

response = lambda_client.create_function(
    FunctionName="S3TriggerLambda",
    Runtime="python3.8",
    Role=role["Role"]["Arn"],
    Handler="lambda_trigger.lambda_handler",
    Code={"ZipFile": lambda_code}
)
print("Lambda function created successfully!")

# Deploy Step Function
stepfunctions_client = boto3.client("stepfunctions")

with open("workflows/step_function.json") as f:
    step_function_definition = json.load(f)

response = stepfunctions_client.create_state_machine(
    name="SCD_ETL_Workflow",
    roleArn=role["Role"]["Arn"],
    definition=json.dumps(step_function_definition)
)

print("Step Function created successfully!")
