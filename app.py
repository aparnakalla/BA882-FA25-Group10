import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os

# ----------------------------------------------------------
# PAGE CONFIG
# ----------------------------------------------------------
st.set_page_config(page_title="MBTA Dashboard", page_icon="🚆", layout="wide")
st.title("🚆 MBTA Data Dashboard")
st.caption("Connected to Google BigQuery")

# ----------------------------------------------------------
# GCP AUTHENTICATION (LOCAL + CLOUD SAFE)
# ----------------------------------------------------------
def get_bigquery_client():
    """Handles both local JSON key and Streamlit Cloud secrets"""
    key_path = os.path.join(os.path.dirname(__file__), "christina-ba882-fall25-d5136a973995.json")
    credentials = None

    if os.path.exists(key_path):
        credentials = service_account.Credentials.from_service_account_file(key_path)
        st.sidebar.success("✅ Using local service account file.")
    elif "gcp_service_account" in st.secrets:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        st.sidebar.success("✅ Using Streamlit Cloud secrets.")
    else:
        st.sidebar.error("❌ No credentials found.")
        st.stop()

    return bigquery.Client(credentials=credentials, project=credentials.project_id)

client = get_bigquery_client()
dataset = "mbta_data"

# ----------------------------------------------------------
# LOAD DATA
# ----------------------------------------------------------
@st.cache_data(ttl=600)
def load_table(dataset, table):
    query = f"SELECT * FROM `{client.project}.{dataset}.{table}` LIMIT 1000"
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error querying {table}: {e}")
        return pd.DataFrame()

# ----------------------------------------------------------
# TABS
# ----------------------------------------------------------
tab1, tab2, tab3 = st.tabs(["🚉 Routes", "⚠️ Alerts", "🗺️ Patterns"])

with tab1:
    st.subheader("MBTA Routes")
    df = load_table(dataset, "routes_external")
    st.dataframe(df)
    if not df.empty:
        st.download_button("Download CSV", df.to_csv(index=False), "routes.csv")

with tab2:
    st.subheader("MBTA Alerts")
    df = load_table(dataset, "alerts_external")
    st.dataframe(df)
    if not df.empty:
        st.download_button("Download CSV", df.to_csv(index=False), "alerts.csv")

with tab3:
    st.subheader("MBTA Route Patterns")
    df = load_table(dataset, "route_patterns_external")
    st.dataframe(df)
    if not df.empty:
        st.download_button("Download CSV", df.to_csv(index=False), "patterns.csv")

st.success("✅ Ready!")
