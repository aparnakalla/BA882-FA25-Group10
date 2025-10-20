import requests
import pandas as pd
import os

def fetch_mbta_routes():
    """
    Fetch routes data from MBTA v3 API using the provided API key.
    Returns a pandas DataFrame.
    """
    api_key = os.getenv("MBTA_API_KEY")
    url = "https://api-v3.mbta.com/routes"
    headers = {"x-api-key": api_key}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    # Normalize JSON into a flat table
    df = pd.json_normalize(data["data"])
    df["fetched_at_utc"] = pd.Timestamp.utcnow()
    return df
