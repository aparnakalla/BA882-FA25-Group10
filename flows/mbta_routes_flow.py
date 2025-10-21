from prefect import flow, task 
from utils.mbta_api import fetch_mbta_routes 
from utils.transform_utils import transform_routes 
from utils.gcs_utils import upload_to_gcs 
import pandas as pd 
BUCKET_NAME = "ba882-team10-bucket" # replace with your actual GCS bucket name 
@task 
def extract():
    return fetch_mbta_routes() 
@task 
def transform(df): 
    return transform_routes(df) 
@task 
def load(df): 
    file_path = f"mbta/routes/routes_snapshot_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet" 
    upload_to_gcs(df, BUCKET_NAME, file_path) 
@flow(name="MBTA Routes Pipeline") 
def mbta_routes_flow(): 
    raw_df = extract() 
    clean_df = transform(raw_df) 
    load(clean_df) 
if __name__ == "__main__": mbta_routes_flow()