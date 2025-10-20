from google.cloud import storage
from io import BytesIO

def upload_to_gcs(df, bucket_name, destination_blob):
    """
    Upload a pandas DataFrame as a parquet file to GCS.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)

    # Convert DataFrame to bytes (parquet)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    blob.upload_from_file(buffer, content_type="application/octet-stream")
    print(f"✅ Uploaded {destination_blob} to bucket {bucket_name}")
