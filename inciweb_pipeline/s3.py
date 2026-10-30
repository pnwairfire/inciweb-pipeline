"""S3 client helpers for the Inciweb pipeline."""

import os

import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
INCIWEB_BUCKET = os.getenv("INCIWEB_BUCKET")


_S3_CLIENT = None


def init_s3():
    global _S3_CLIENT
    if _S3_CLIENT is not None:
        return _S3_CLIENT

    endpoint_url = os.getenv("AWS_ENDPOINT_URL")
    kwargs = {}
    if AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY:
        kwargs["aws_access_key_id"] = AWS_ACCESS_KEY
        kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url

    _S3_CLIENT = boto3.client("s3", **kwargs)
    return _S3_CLIENT


def inciweb_bucket():
    raw = (os.getenv("INCIWEB_BUCKET") or INCIWEB_BUCKET or "").strip()
    return raw.split("/", 1)[0] if "/" in raw else raw


def inciweb_s3_target(inciweb_id: str) -> tuple[str, str]:
    raw_bucket = (os.getenv("INCIWEB_BUCKET") or INCIWEB_BUCKET or "").strip()
    explicit_prefix = os.getenv("INCIWEB_S3_PREFIX", "").strip().strip("/")

    if "/" in raw_bucket:
        bucket, bucket_prefix = raw_bucket.split("/", 1)
        bucket_prefix = bucket_prefix.strip("/")
    else:
        bucket, bucket_prefix = raw_bucket, ""

    prefix = explicit_prefix or bucket_prefix
    key = (
        f"{prefix}/{inciweb_id}/latest/payload.json"
        if prefix
        else f"{inciweb_id}/latest/payload.json"
    )
    return bucket, key
