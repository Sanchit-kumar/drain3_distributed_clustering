    
import pandas as pd
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from common.config import S3_BUCKET_ENDPOINT, S3_BUCKET_NAME, S3_ACCESS_KEY, S3_SECRET_KEY


# FIX 1: Added addressing_style='path' for MinIO compatibility
s3_client = boto3.client(
    's3',
    endpoint_url=S3_BUCKET_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}),
    region_name='us-east-1'
)

def ensure_bucket_exists():
    """Checks if bucket exists; creates it if not."""
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
    except ClientError:
        print(f"Creating bucket: {S3_BUCKET_NAME}")
        s3_client.create_bucket(Bucket=S3_BUCKET_NAME)

def read_parquet_from_s3(file_key):
    """Reads a Parquet file directly from MinIO using native s3fs."""
    try:
        # FIX 2: Eliminated the Boto3 get_object memory trap
        return pd.read_parquet(
            f"s3://{S3_BUCKET_NAME}/{file_key}",
            storage_options={
                "client_kwargs": {"endpoint_url": S3_BUCKET_ENDPOINT},
                "key": S3_ACCESS_KEY,
                "secret": S3_ACCESS_KEY            }
        )
    except Exception as e:
        print(f"❌ Error reading from S3: {e}")
        return None

def upload_to_s3_and_get_static_link(df, file_key):
    """Uploads DF directly to S3 as Parquet, enforcing safe row groups."""
    ensure_bucket_exists() 
    
    s3_path = f"s3://{S3_BUCKET_NAME}/{file_key}"
    
    # FIX 3: Streams directly to S3, honoring the row_group_size without RAM spikes
    df.to_parquet(
        s3_path, 
        index=False, 
        engine='pyarrow', 
        row_group_size=100000,
        storage_options={
            "client_kwargs": {"endpoint_url": S3_BUCKET_ENDPOINT},
            "key": S3_ACCESS_KEY,
            "secret": S3_SECRET_KEY
        }
    )
    
    return f"{S3_BUCKET_ENDPOINT}/{S3_BUCKET_NAME}/{file_key}"




path = '/app/HDFS_2k.csv'
df = pd.read_csv(path)
n = 1
df = pd.concat([df]*n)
# Upload and get link
link = upload_to_s3_and_get_static_link(df, "data.parquet")
print(f"✅ Permanent Link: {link}")
print(len(df))