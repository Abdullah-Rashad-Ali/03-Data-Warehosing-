import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time

# ---------------- CONFIG ----------------
PROJECT_ID = "qwiklabs-gcp-00-2d010dbe49d6"
BUCKET_NAME = "dezoomcamp-hw3-2025-abdullah"  # MUST be globally unique, lowercase, no _
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)]
DOWNLOAD_DIR = "."
CHUNK_SIZE = 8 * 1024 * 1024
# ----------------------------------------

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ✅ Use Application Default Credentials (works inside GCP)
client = storage.Client(project=PROJECT_ID)


def create_bucket(bucket_name):
    try:
        bucket = client.get_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' already exists. Using it.")
    except NotFound:
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        print(
            f"Bucket '{bucket_name}' exists but you do not have access. "
            "Choose a different bucket name."
        )
        sys.exit(1)

    return bucket


def download_file(month):
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def verify_gcs_upload(bucket, blob_name):
    return bucket.blob(blob_name).exists(client)


def upload_to_gcs(bucket, file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)

            if verify_gcs_upload(bucket, blob_name):
                print(f"Uploaded & verified: gs://{BUCKET_NAME}/{blob_name}")
                return
            else:
                print("Verification failed, retrying...")
        except Exception as e:
            print(f"Upload failed: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")


if __name__ == "__main__":
    # ✅ Create bucket once
    bucket = create_bucket(BUCKET_NAME)

    # Download files
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))

    # Upload files
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(
            lambda fp: upload_to_gcs(bucket, fp),
            filter(None, file_paths),
        )

    print("All files processed and uploaded successfully.")
