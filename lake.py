import pyarrow.fs as pafs

# local-dev credentials; we'll move these to environment variables later
ENDPOINT = "localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"

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