from prefect import flow, task
from utils.mbta_api import fetch_mbta_vehicles
from utils.transform_utils import transform_vehicles
from utils.gcs_utils import upload_to_gcs
import pandas as pd

BUCKET_NAME = "ba882-team10-bucket"

@task
def extract():
    return fetch_mbta_vehicles()

@task
def transform(df):
    return transform_vehicles(df)

@task
def load(df):
    file_path = f"mbta/vehicles/vehicles_snapshot_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"
    upload_to_gcs(df, BUCKET_NAME, file_path)

@flow(name="MBTA Vehicles Pipeline")
def mbta_vehicles_flow():
    raw_df = extract()
    clean_df = transform(raw_df)
    load(clean_df)

if __name__ == "__main__":
    mbta_vehicles_flow()
