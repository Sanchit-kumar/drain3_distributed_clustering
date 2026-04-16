import os
import logging

# Setup structured logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("drain3_app")

# S3 / MinIO Config
ENDPOINT_URL = "http://localhost:9000"
BUCKET = "my-bucket-name"


S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://host.docker.internal:9000")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password123")
BUCKET_NAME = os.getenv("BUCKET_NAME", "my-bucket-name")

# Redis Config
# REDIS_HOST = os.getenv("REDIS_HOST", "host.docker.internal")
# REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
# REDIS_QUEUE_NAME = os.getenv("REDIS_QUEUE_NAME", "drain_tasks")
# REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "yourpassword123")