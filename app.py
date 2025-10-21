import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="MBTA Data Dashboard",
    page_icon="🚆",
    layout="wide"
)

# --- GCP AUTHENTICATION ---
key_path = "christina-ba882-fall25-d5136a973995.json"
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# --- HELPER FUNCTION TO LOAD DATA ---
@st.cache_data(ttl=600)
def load_table(dataset, table):
    query = f"SELECT * FROM `{credentials.project_id}.{dataset}.{table}` LIMIT 1000"
    df = client.query(query).to_dataframe()
    return df

# --- SIDEBAR ---
st.sidebar.header("MBTA Data Dashboard 🚆")
st.sidebar.markdown("Explore real-time data from MBTA APIs stored in BigQuery.")
dataset = "mbta_data"

# --- MAIN DASHBOARD TABS ---
tab1, tab2, tab3 = st.tabs(["🚉 Routes", "⚠️ Alerts", "🗺️ Route Patterns"])

with tab1:
    st.subheader("MBTA Routes")
    routes_df = load_table(dataset, "routes_external")
    st.dataframe(routes_df, use_container_width=True)
    st.download_button("Download Routes CSV", routes_df.to_csv(index=False), "routes.csv")

with tab2:
    st.subheader("MBTA Alerts")
    alerts_df = load_table(dataset, "alerts_external")
    st.dataframe(alerts_df, use_container_width=True)
    st.download_button("Download Alerts CSV", alerts_df.to_csv(index=False), "alerts.csv")

with tab3:
    st.subheader("MBTA Route Patterns")
    route_patterns_df = load_table(dataset, "route_patterns_external")
    st.dataframe(route_patterns_df, use_container_width=True)
    st.download_button("Download Route Patterns CSV", route_patterns_df.to_csv(index=False), "route_patterns.csv")


st.success("✅ Data loaded successfully from BigQuery!")
