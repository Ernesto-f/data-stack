import os
import boto3

def s3_client():
    endpoint = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
    access = os.getenv("S3_ACCESS_KEY", "minio")
    secret = os.getenv("S3_SECRET_KEY", "minio123")
    region = os.getenv("S3_REGION", "us-east-1")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name=region,
    )
