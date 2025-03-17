# SCD-and-Incremental-Load-Implementation
Implement Slowly Changing Dimensions and Incremental Load using AWS Glue, Lambda and Step Functions


This project builds an AWS ETL pipeline that:

✅ Uses SCD 1, 2, and 3 for slowly changing dimensions

✅ Implements incremental data load to avoid duplicates

✅ Runs on AWS Glue, Lambda, Step Functions, S3, and Redshift

✅ Deploys automatically using Boto3


NOTE:-

This scd and incremental load pipeline has all the essential steps and scripts to implement another use-case with a little tinkering and following the below steps:-
1. Edit the glue_etl.py script according to your use case for the columns, data and tables in use.
2. Set up an AWS account with
     - Enabled S3, glue, lambda, step functions etc.
     - Create IAM roles.
     - Installed python 3.8+
     - Installed boto3 and CLI for deployment.
  3. In the config.json file replace in the given placeholders put your s3 bucket path, jdbc url, redshift username and redshift password.

Following these set up your pipeline and it should be ready to run. 
