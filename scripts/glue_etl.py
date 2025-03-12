
import json
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, current_timestamp, max as spark_max

# Load configuration
with open("config/config.json") as f:
    config = json.load(f)

s3_bucket = config["s3_bucket"]
jdbc_url = config["redshift_jdbc_url"]
db_properties = {"user": config["redshift_user"], "password": config["redshift_password"]}

# Initialize Spark
spark = SparkSession.builder.appName("AWS Glue Incremental ETL").getOrCreate()

# Step 1: Get the last processed timestamp from Redshift
last_processed_query = "(SELECT COALESCE(MAX(last_updated), '1900-01-01') FROM etl_checkpoint)"  
last_processed_df = spark.read.jdbc(url=jdbc_url, table=last_processed_query, properties=db_properties)
last_processed_time = last_processed_df.collect()[0][0]

# Step 2: Read new/updated data from S3 (Incremental Load)
new_data = spark.read.csv(f"s3://{s3_bucket}/customer_data.csv", header=True, inferSchema=True)
incremental_data = new_data.filter(col("modified_timestamp") > last_processed_time)

# Step 3: Read existing data from Redshift
existing_data = spark.read.jdbc(url=jdbc_url, table="customer_dim", properties=db_properties)

# Step 4: Apply Slowly Changing Dimensions (SCD 1, 2, and 3)

## --- SCD TYPE 1: Overwrite Existing Records ---
scd1_data = incremental_data.alias("new").join(existing_data.alias("old"), "customer_id", "left") \
    .select(
        col("new.customer_id"),
        col("new.name"),
        col("new.city"),
        col("new.current_job"),
        col("new.previous_job"),
        col("new.modified_timestamp").alias("last_updated")
    )

## --- SCD TYPE 2: Maintain History ---
scd2_updates = incremental_data.alias("new").join(existing_data.alias("old"), "customer_id") \
    .filter((col("new.city") != col("old.city")) | (col("new.current_job") != col("old.current_job"))) \
    .select(
        col("old.customer_id"),
        col("old.name"),
        col("old.city"),
        col("old.current_job"),
        col("old.previous_job"),
        col("old.start_date"),
        current_timestamp().alias("end_date"),
        lit(False).alias("is_active")
    )

scd2_new_entries = incremental_data \
    .select(
        col("customer_id"),
        col("name"),
        col("city"),
        col("current_job"),
        col("previous_job"),
        current_timestamp().alias("start_date"),
        lit(None).alias("end_date"),
        lit(True).alias("is_active")
    )

## --- SCD TYPE 3: Keep the previous value in separate columns ---
scd3_data = incremental_data.alias("new").join(existing_data.alias("old"), "customer_id", "left") \
    .select(
        col("new.customer_id"),
        col("new.name"),
        col("new.city").alias("current_city"),
        col("old.city").alias("previous_city"),
        col("new.current_job"),
        col("old.current_job").alias("previous_job"),
        col("new.modified_timestamp").alias("last_updated")
    )

# Step 5: Write Incremental Data to Redshift
scd1_data.write.jdbc(url=jdbc_url, table="customer_dim", mode="append", properties=db_properties)
scd2_updates.union(scd2_new_entries).write.jdbc(url=jdbc_url, table="customer_dim", mode="append", properties=db_properties)
scd3_data.write.jdbc(url=jdbc_url, table="customer_dim", mode="append", properties=db_properties)

# Step 6: Update the Checkpoint Table
latest_time = incremental_data.agg(spark_max("modified_timestamp")).collect()[0][0]
checkpoint_df = spark.createDataFrame([(latest_time,)], ["last_updated"])
checkpoint_df.write.jdbc(url=jdbc_url, table="etl_checkpoint", mode="overwrite", properties=db_properties)

print("Incremental ETL process completed successfully!")
