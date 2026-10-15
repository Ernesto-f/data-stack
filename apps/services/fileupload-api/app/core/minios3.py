import os
import boto3
from botocore.client import Config
from app.core.config import settings

_S3 = None

def s3_client():
    global _S3
    if _S3 is not None:
        return _S3

    endpoint = settings.S3_ENDPOINT            
    access_key = settings.S3_ACCESS_KEY
    secret_key = settings.S3_SECRET_KEY
    region = settings.S3_REGION

    if not endpoint or not access_key or not secret_key:
        raise RuntimeError("Vars S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY são obrigatórias.")

    _S3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4"),
    )
    return _S3

def upload_fileobj(bucket: str, key: str, fileobj, content_type: str | None = None):
    extra = {}
    if content_type:
        extra["ContentType"] = content_type

    s3 = s3_client()
    s3.upload_fileobj(fileobj, bucket, key, ExtraArgs=extra if extra else None)
