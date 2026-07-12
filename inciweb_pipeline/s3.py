"""S3 client helpers for the Inciweb pipeline."""

import os

import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
INCIWEB_BUCKET = os.getenv("INCIWEB_BUCKET")


def init_s3():
    endpoint_url = os.getenv("AWS_ENDPOINT_URL")
    kwargs = {}
    if AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY:
        kwargs["aws_access_key_id"] = AWS_ACCESS_KEY
        kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url

    return boto3.client("s3", **kwargs)


def inciweb_bucket():
    return INCIWEB_BUCKET
