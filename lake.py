import os

import pyarrow.fs as pafs
from dotenv import load_dotenv

load_dotenv()   # finds the .env file next to this script, whatever folder you run from

ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
ACCESS_KEY = os.environ["MINIO_ROOT_USER"]        # crashes loudly if .env is missing
SECRET_KEY = os.environ["MINIO_ROOT_PASSWORD"]

BUCKET = "clicks-lake"
RAW_PREFIX = f"{BUCKET}/raw"   # raw/date=YYYY-MM-DD/clicks_<ms>.parquet


def get_fs():
    """An S3 filesystem pointed at MinIO instead of AWS."""
    return pafs.S3FileSystem(
        endpoint_override=ENDPOINT,
        scheme="http",
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        region="us-east-1",
        allow_bucket_creation=True,
    )