from prefect import flow, task
from utils.mbta_api import fetch_mbta_alerts
from utils.transform_utils import transform_alerts
from utils.gcs_utils import upload_to_gcs
import pandas as pd

BUCKET_NAME = "ba882-team10-bucket"

@task
def extract():
    return fetch_mbta_alerts()

@task
def transform(df):
    return transform_alerts(df)

@task
def load(df):
    file_path = f"mbta/alerts/alerts_snapshot_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"
    upload_to_gcs(df, BUCKET_NAME, file_path)

@flow(name="MBTA Alerts Pipeline")
def mbta_alerts_flow():
    raw_df = extract()
    clean_df = transform(raw_df)
    load(clean_df)

if __name__ == "__main__":
    mbta_alerts_flow()
