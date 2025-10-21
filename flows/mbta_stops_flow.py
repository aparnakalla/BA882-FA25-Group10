from prefect import flow, task
from utils.mbta_api import fetch_mbta_stops
from utils.transform_utils import transform_stops
from utils.gcs_utils import upload_to_gcs
import pandas as pd

BUCKET_NAME = "ba882-team10-bucket"

@task
def extract():
    return fetch_mbta_stops()

@task
def transform(df):
    return transform_stops(df)

@task
def load(df):
    file_path = f"mbta/stops/stops_snapshot_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"
    upload_to_gcs(df, BUCKET_NAME, file_path)

@flow(name="MBTA Stops Pipeline")
def mbta_stops_flow():
    raw_df = extract()
    clean_df = transform(raw_df)
    load(clean_df)

if __name__ == "__main__":
    mbta_stops_flow()
