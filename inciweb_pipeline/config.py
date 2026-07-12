"""Externalized configuration for Inciweb data."""

import os

from dotenv import load_dotenv

load_dotenv()


def _env(key, default):
    return os.getenv(key, default)


# --- S3 Buckets and Keys ---
INCIWEB_BUCKET = _env("INCIWEB_BUCKET", "")
NEAR_ME_BASE_URL = _env("NEAR_ME_BASE_URL", "http://mock-service.prefect-af.local:8000")
