from prefect import flow, task
from utils.mbta_api import fetch_mbta_routes
from utils.transform_utils import transform_routes
from utils.gcs_utils import upload_to_gcs
import pandas as pd
from io import StringIO

BUCKET_NAME = "ba882-team10-bucket"  # replace with your actual GCS bucket name

@task
def extract():
    return fetch_mbta_routes()

@task
def transform(df):
    return transform_routes(df)

@task
def load(df):
    # Generate file path with timestamp
    file_path = f"mbta/routes/routes_snapshot_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}.jsonl"
    
    # Convert DataFrame to JSON Lines string
    json_data = df.to_json(orient="records", lines=True)
    
    # Convert string to bytes before uploading
    upload_to_gcs(json_data.encode("utf-8"), BUCKET_NAME, file_path)

@flow(name="MBTA Routes Pipeline")
def mbta_routes_flow():
    raw_df = extract()
    clean_df = transform(raw_df)
    load(clean_df)

if __name__ == "__main__":
    mbta_routes_flow()
