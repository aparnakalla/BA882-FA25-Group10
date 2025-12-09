# app.py
import os
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import numpy as np

# ----------------------------- CONFIG -----------------------------
st.set_page_config(page_title="MBTA Dashboard", page_icon="🚆", layout="wide")
st.title("🚆 MBTA Data Dashboard")
st.caption("Backed by Google BigQuery")

DEFAULT_DATASET = "data_from_gcs_to_bq"
DEFAULT_PROJECT = os.getenv("GCP_PROJECT")

# ------------------- AUTH -------------------
# def get_bq_client():
#     key_path = os.path.join(os.path.dirname(__file__), "christina-ba882-fall25-b1d451bd7f68.json")
#     credentials = None
#     project_id = DEFAULT_PROJECT

#     if os.path.exists(key_path):
#         credentials = service_account.Credentials.from_service_account_file(key_path)
#         project_id = project_id or credentials.project_id
#         st.sidebar.success("✅ Using local service account file.")
#     elif "gcp_service_account" in st.secrets:
#         credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
#         project_id = project_id or credentials.project_id
#         st.sidebar.success("✅ Using Streamlit Cloud secrets.")
#     else:
#         st.sidebar.error("❌ No GCP credentials found.")
#         st.stop()

#     return bigquery.Client(project=project_id, credentials=credentials), project_id
import os, json, base64
from google.oauth2 import service_account
from google.cloud import bigquery
import streamlit as st

def get_bq_client():
    # 1. Local mode (Streamlit secrets available)
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
        else:
            raise KeyError("No local Streamlit secrets")
    except:
        # 2. Cloud Run mode (fallback to env variable)
        encoded = os.environ["GCP_SERVICE_ACCOUNT"]
        decoded = base64.b64decode(encoded)
        info = json.loads(decoded)

    credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(credentials=credentials, project=info["project_id"])
    return client, info["project_id"]

client, PROJECT_ID = get_bq_client()

# -------------------------- HELPERS --------------------------
@st.cache_data(ttl=600, show_spinner=False)
def list_tables(dataset: str) -> pd.DataFrame:
    sql = f"""
    SELECT table_name
    FROM `{PROJECT_ID}.{dataset}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name NOT LIKE '%_ext'
    ORDER BY table_name
    """
    return client.query(sql).result().to_dataframe()

@st.cache_data(ttl=600, show_spinner=False)
def preview_table(dataset: str, table: str, limit: int = 20000) -> pd.DataFrame:
    sql = f"SELECT * FROM `{PROJECT_ID}.{dataset}.{table}` LIMIT {int(limit)}"
    try:
        return client.query(sql).result().to_dataframe()
    except Exception as e:
        st.error(f"Error loading {table}: {e}")
        return pd.DataFrame()

def numeric_columns(df: pd.DataFrame):
    return [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

def safe_map(df: pd.DataFrame, lat_col: str, lon_col: str):
    """Safely convert coordinate columns to float and drop invalid rows."""
    try:
        map_df = df[[lat_col, lon_col]].copy()
        map_df[lat_col] = pd.to_numeric(map_df[lat_col], errors="coerce")
        map_df[lon_col] = pd.to_numeric(map_df[lon_col], errors="coerce")
        map_df = map_df.dropna(subset=[lat_col, lon_col])
        map_df = map_df.rename(columns={lat_col: "lat", lon_col: "lon"})
        if not map_df.empty:
            st.map(map_df)
        else:
            st.info("No valid latitude/longitude data to display.")
    except Exception as e:
        st.warning(f"Map not shown: {e}")

# -------------------------- SIDEBAR --------------------------
st.sidebar.header("Settings")
dataset_name = st.sidebar.text_input("BigQuery dataset", value=DEFAULT_DATASET)
refresh = st.sidebar.button("🔄 Refresh tables")

if refresh:
    list_tables.clear()
    preview_table.clear()

tbls_df = list_tables(dataset_name)

with st.sidebar.expander("ℹ️ Tips"):
    st.markdown(
        "- Tabs show MBTA data by category.\n"
        "- Use SQL Lab for custom queries.\n"
        "- All tables limited to 20K rows for performance."
    )

# -------------------------- TABS --------------------------
tab_overview, tab_lines, tab_routes, tab_facilities, tab_predictions, tab_schedules, tab_shapes, tab_stops, tab_trips, tab_vehicles, tab_patterns, tab_alerts,tab_delay, tab_sql = st.tabs(
    ["📊 Overview", "🧵 Lines", "🛤 Routes", "🏢 Facilities", "📈 Predictions", "🗓 Schedules", "🌀 Shapes", "🛑 Stops", "🚌 Trips", "🚗 Vehicles", "🗺 Patterns", "⚠ Alerts", "⏱ Delay Analytics","🧪 SQL Lab"]
)

# -------------------------- OVERVIEW --------------------------
with tab_overview:
    st.subheader("Dataset Overview")
    if tbls_df.empty:
        st.warning(f"No tables found in `{PROJECT_ID}.{dataset_name}`.")
    else:
        st.dataframe(tbls_df, use_container_width=True, hide_index=True)
        st.metric("Tables Found", len(tbls_df))

# -------------------------- LINES --------------------------
with tab_lines:
    st.subheader("Lines")
    df = preview_table(dataset_name, "lines", 5000)

    if df.empty:
        st.info("No data.")
    else:
        # ----- Summary Metrics -----
        c1, c2, c3 = st.columns(3)
        c1.metric("Rows", f"{len(df):,}")
        c2.metric("Distinct Long Names", df["long_name"].nunique() if "long_name" in df.columns else 0)
        c3.metric("Unique Colors", df["color"].nunique() if "color" in df.columns else 0)

        st.dataframe(df, use_container_width=True, hide_index=True)

        # =================== ANALYTICAL INSIGHTS ===================
        st.markdown("### 🔍 Key Network Insights")

        # 1️⃣ Identify line naming patterns (e.g., Green Line variants)
        if "long_name" in df.columns:
            green_variants = df[df["long_name"].str.contains("Green", case=False, na=False)]
            st.write(f"**Green Line Variants:** {len(green_variants)} (e.g., {', '.join(green_variants['long_name'].head(5))}...)")

            grouped = df["long_name"].str.extract(r"([A-Za-z]+)")  # extract main color/name
            top_groups = grouped[0].value_counts().head(10)
            st.bar_chart(top_groups)
            st.caption("Top naming groups — subway lines (Red, Orange, Green, Blue) dominate the schema.")

        # 2️⃣ Color naming consistency
        if "color" in df.columns and "long_name" in df.columns:
            bad_colors = df[df["color"].isna() | (df["color"] == "")]
            if bad_colors.empty:
                st.success("✅ All lines have valid color assignments.")
            else:
                st.warning(f"⚠ {len(bad_colors)} lines missing color codes.")
                st.dataframe(bad_colors)

        # 3️⃣ Cross-tab: color vs. name
        if "color" in df.columns and "long_name" in df.columns:
            st.markdown("### 🎨 Color–Name Relationship")
            color_map = (
                df.groupby("color")["long_name"]
                .nunique()
                .reset_index()
                .sort_values("long_name", ascending=False)
            )
            st.bar_chart(color_map.set_index("color"))
            st.caption("Shows how many unique long_names share the same color (e.g., multiple Silver Line branches use one color).")

        # 4️⃣ Data health snapshot
        st.markdown("### 🧮 Data Quality Snapshot")
        missing_vals = df.isna().sum().reset_index()
        missing_vals.columns = ["Column", "Missing Count"]
        st.dataframe(missing_vals)
        st.caption("Null counts help validate ETL completeness before feeding into route-level analytics.")

        # 5️⃣ Quick Summary
        st.markdown("### 💡 Quick Summary")
        st.write(f"- The dataset lists **{len(df)} line records** across **{df['color'].nunique()} system colors.**")
        if "type" in df.columns:
            st.write(f"- All rows currently have type `{df['type'].unique()[0]}` — standardized schema import.")
        st.write("- Color–name patterns align with MBTA design conventions: distinct colors per major rapid transit branch.")
        st.write("- Data appears structurally complete with minimal missing fields.")

        # ----- Download -----
        st.download_button("Download CSV", df.to_csv(index=False), "lines.csv")




# -------------------------- ROUTES (from routes_ext) --------------------------
# -------------------------- ROUTES (from routes_ext) --------------------------
with tab_routes:
    st.subheader("🛤 MBTA Routes – Extended Data")
    table = "routes_ext"
    st.caption(f"Using table: `{table}`")

    try:
        sql = f"""
        SELECT
          data.id AS route_id,
          data.attributes.long_name AS long_name,
          data.attributes.short_name AS short_name,
          data.attributes.color AS color,
          data.attributes.text_color AS text_color,
          data.attributes.description AS description,
          data.attributes.type AS route_type,
          data.attributes.sort_order AS sort_order,
          data.attributes.fare_class AS fare_class
        FROM `{PROJECT_ID}.{dataset_name}.{table}`,
             UNNEST(data) AS data
        """
        df = client.query(sql).result().to_dataframe()

        if df.empty:
            st.info("No data found in routes_ext.")
        else:
            st.metric("Rows", f"{len(df):,}")
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.download_button("📥 Download Flattened Routes Data", df.to_csv(index=False), "routes_ext.csv")

    except Exception as e:
        st.error(f"routes_ext error: {e}")



# -------------------------- FACILITIES --------------------------
with tab_facilities:
    st.subheader("Facilities")
    df = preview_table(dataset_name, "facilities", 10000)
    if df.empty:
        st.info("No data.")
    else:
        st.metric("Rows", f"{len(df):,}")
        lat_col = [c for c in df.columns if "lat" in c.lower()]
        lon_col = [c for c in df.columns if "lon" in c.lower()]
        if lat_col and lon_col:
            safe_map(df, lat_col[0], lon_col[0])
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button("Download CSV", df.to_csv(index=False), "facilities.csv")

# -------------------------- PREDICTIONS (from predictions_ext) --------------------------
# -------------------------- PREDICTIONS (from predictions_ext) --------------------------
with tab_predictions:
    st.subheader("📈 Real-Time Predictions – Extended Data")
    table = "predictions_ext"
    st.caption(f"Using table: `{table}`")

    try:
        sql = f"""
        SELECT
          data.id AS prediction_id,
          data.relationships.route.data.id AS route_id,
          data.relationships.stop.data.id AS stop_id,
          data.relationships.trip.data.id AS trip_id,
          data.attributes.direction_id AS direction_id,
          data.attributes.departure_time AS departure_time,
          data.attributes.arrival_time AS arrival_time,
          data.attributes.departure_uncertainty AS departure_uncertainty,
          data.attributes.arrival_uncertainty AS arrival_uncertainty,
          data.attributes.update_type AS update_type
        FROM `{PROJECT_ID}.{dataset_name}.{table}`,
             UNNEST(data) AS data
        """
        df = client.query(sql).result().to_dataframe()

        if df.empty:
            st.info("No data found in predictions_ext.")
        else:
            st.metric("Rows", f"{len(df):,}")
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.download_button("📥 Download Flattened Predictions Data", df.to_csv(index=False), "predictions_ext.csv")

    except Exception as e:
        st.error(f"predictions_ext error: {e}")



# -------------------------- SCHEDULES --------------------------
with tab_schedules:
    st.subheader("Schedules")
    df = preview_table(dataset_name, "schedules", 10000)
    if df.empty:
        st.info("No data.")
    else:
        st.metric("Rows", f"{len(df):,}")
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button("Download CSV", df.to_csv(index=False), "schedules.csv")

# -------------------------- SHAPES --------------------------
with tab_shapes:
    st.subheader("Shapes")
    df = preview_table(dataset_name, "shapes", 10000)
    if df.empty:
        st.info("No data.")
    else:
        st.metric("Rows", f"{len(df):,}")
        lat_col = [c for c in df.columns if "lat" in c.lower()]
        lon_col = [c for c in df.columns if "lon" in c.lower()]
        if lat_col and lon_col:
            safe_map(df, lat_col[0], lon_col[0])
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button("Download CSV", df.to_csv(index=False), "shapes.csv")

# -------------------------- STOPS --------------------------
with tab_stops:
    st.subheader("Stops")
    df = preview_table(dataset_name, "stops", 20000)
    if df.empty:
        st.info("No data.")
    else:
        st.metric("Total Stops", f"{len(df):,}")
        lat_col = [c for c in df.columns if "lat" in c.lower()]
        lon_col = [c for c in df.columns if "lon" in c.lower()]
        if lat_col and lon_col:
            safe_map(df, lat_col[0], lon_col[0])
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button("Download CSV", df.to_csv(index=False), "stops.csv")



# -------------------------- TRIPS --------------------------
with tab_trips:
    st.subheader("Trips")
    df = preview_table(dataset_name, "trips", 20000)
    if df.empty:
        st.info("No data.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Rows", f"{len(df):,}")
        if "route_id" in df.columns:
            c2.metric("Distinct Routes", df["route_id"].nunique())
        if "direction_id" in df.columns:
            c3.metric("Directions", df["direction_id"].nunique())
        if "wheelchair_accessible" in df.columns:
            c4.metric("Wheelchair Accessible Trips", df["wheelchair_accessible"].sum())

        st.dataframe(df, use_container_width=True, hide_index=True)

        # ---------- EDA VISUALS ----------
        st.markdown("### 🧭 Trip Distribution Insights")

        # 1️⃣ Trips by Route
        if "route_id" in df.columns:
            route_counts = df["route_id"].value_counts().head(15).reset_index()
            route_counts.columns = ["Route", "Trip Count"]
            st.bar_chart(route_counts.set_index("Route"))

        # 2️⃣ Direction Split
        if "direction_id" in df.columns:
            dir_counts = df["direction_id"].value_counts().reset_index()
            dir_counts.columns = ["Direction", "Count"]
            st.subheader("Direction Split")
            st.dataframe(dir_counts)
            st.bar_chart(dir_counts.set_index("Direction"))

        # 3️⃣ Top Headsigns (destination indicators)
        if "headsign" in df.columns:
            st.subheader("Top 10 Trip Headsigns")
            headsign_counts = df["headsign"].value_counts().head(10)
            st.bar_chart(headsign_counts)

        # 4️⃣ Accessibility Ratio
        if "wheelchair_accessible" in df.columns:
            st.subheader("Accessibility Breakdown")
            access_counts = df["wheelchair_accessible"].value_counts().reset_index()
            access_counts.columns = ["Wheelchair Accessible (1=Yes)", "Count"]
            st.bar_chart(access_counts.set_index("Wheelchair Accessible (1=Yes)"))

        # 5️⃣ Bikes Allowed
        if "bikes_allowed" in df.columns:
            st.subheader("Bikes Allowed")
            bike_counts = df["bikes_allowed"].value_counts().reset_index()
            bike_counts.columns = ["Bikes Allowed (1=Yes)", "Count"]
            st.bar_chart(bike_counts.set_index("Bikes Allowed (1=Yes)"))

        st.download_button("Download CSV", df.to_csv(index=False), "trips.csv")


# -------------------------- VEHICLES --------------------------
with tab_vehicles:
    st.subheader("Vehicles")
    df = preview_table(dataset_name, "vehicles", 10000)
    if df.empty:
        st.info("No data.")
    else:
        st.metric("Vehicles", f"{len(df):,}")
        lat_col = [c for c in df.columns if "lat" in c.lower()]
        lon_col = [c for c in df.columns if "lon" in c.lower()]
        if lat_col and lon_col:
            safe_map(df, lat_col[0], lon_col[0])
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button("Download CSV", df.to_csv(index=False), "vehicles.csv")


# -------------------------- ROUTE PATTERNS (EXT FLATTENED + EDA + FILTER) --------------------------
# -------------------------- ROUTE PATTERNS (EXT FLATTENED + CLEAN EDA) --------------------------
with tab_patterns:
    st.subheader("Route Patterns Overview")

    table = "route_patterns_ext"
    st.caption(f"Using table: `{table}`")

    try:
        sql = f"""
        SELECT
          data.relationships.route.data.id AS route_id,
          data.attributes.name AS pattern_name,
          data.attributes.canonical AS is_canonical
        FROM `{PROJECT_ID}.{dataset_name}.{table}`,
          UNNEST(data) AS data
        """

        df = client.query(sql).result().to_dataframe()

        if df.empty:
            st.info("No data available in this table.")
        else:
            # --- Summary metrics ---
            c1, c2, c3 = st.columns(3)
            c1.metric("Total Patterns", f"{len(df):,}")
            c2.metric("Distinct Routes", df["route_id"].nunique())
            c3.metric("Canonical Patterns", int(df["is_canonical"].sum()) if "is_canonical" in df else 0)

            # --- Multiselect filter for routes ---
            route_options = sorted(df["route_id"].dropna().unique())
            selected_routes = st.multiselect(
                "🎚️ Select one or more routes to visualize",
                options=route_options,
                default=[]
            )

            # Apply route filter if selected
            if selected_routes:
                df = df[df["route_id"].isin(selected_routes)]

            # --- Patterns per route chart ---
            st.markdown("### 🚆 Patterns per Route")
            st.caption(
                "Each bar represents how many unique route patterns (distinct journey configurations) exist for that MBTA route. "
                "Routes with more patterns typically serve multiple branches, shuttle substitutions, or weekend variants."
            )

            route_ct = df["route_id"].value_counts().reset_index()
            route_ct.columns = ["Route ID", "Patterns"]
            st.bar_chart(route_ct.set_index("Route ID"))

            # --- Optional: Canonical vs Non-Canonical summary if you want to keep it ---
            if df["is_canonical"].notna().any():
                st.markdown("### ✅ Canonical vs Non-Canonical (Optional Diagnostic)")
                canon_df = df.copy()
                canon_df["is_canonical_str"] = canon_df["is_canonical"].astype(str)
                canon_ct = canon_df["is_canonical_str"].value_counts().reset_index()
                canon_ct.columns = ["Canonical", "Count"]
                canon_ct["Canonical"] = canon_ct["Canonical"].replace(
                    {"True": "Canonical", "False": "Non-Canonical", "nan": "Unknown"}
                )
                st.bar_chart(canon_ct.set_index("Canonical"))

            # --- Download option ---
            st.download_button("📥 Download Filtered Data", df.to_csv(index=False), "route_patterns_filtered.csv")

    except Exception as e:
        st.error(f"route_patterns_ext error: {e}")



# -------------------------- ALERTS EXPLORATORY ANALYSIS --------------------------
with tab_alerts:
    st.subheader("🚨 MBTA Service Alerts – Insights")

    table = "alerts"
    st.caption(f"Using table: `{table}`")

    try:
        sql = f"""
        SELECT
          id,
          cause,
          effect,
          severity,
          header,
          service_effect,
          lifecycle,
          created_at,
          updated_at
        FROM `{PROJECT_ID}.{dataset_name}.{table}`
        """
        df = client.query(sql).result().to_dataframe()

        if df.empty:
            st.info("No data available in this table.")
        else:
            # --- Force datetime conversion safely ---
            for col in ["created_at", "updated_at"]:
                df[col] = pd.to_datetime(
                    df[col].astype(str).str.strip().replace(["None", "null", "NaT", ""], np.nan),
                    errors="coerce",
                    utc=True
                )

            # --- Compute duration only where valid ---
            if ("created_at" in df.columns) and ("updated_at" in df.columns):
                valid_mask = df["created_at"].notna() & df["updated_at"].notna()
                df["duration_hours"] = np.where(
                    valid_mask,
                    (df["updated_at"] - df["created_at"]).dt.total_seconds() / 3600,
                    np.nan
                )
            else:
                df["duration_hours"] = np.nan

            # --- Clean categorical data ---
            df["cause"] = df["cause"].fillna("Unknown")
            df["effect"] = df["effect"].fillna("Unspecified")
            df["severity"] = df["severity"].fillna("None")

            # --- Aggregate alert counts ---
            st.markdown("### 📊 Alert Frequency by Severity and Cause")
            st.caption(
                "This chart shows how often each alert cause occurs, grouped by severity. "
                "It highlights the most common disruption sources and their operational impact."
            )

            agg = (
                df.groupby(["cause", "severity"])
                .size()
                .reset_index(name="Alert Count")
                .sort_values("Alert Count", ascending=False)
            )

            import plotly.express as px
            fig = px.bar(
                agg,
                x="cause",
                y="Alert Count",
                color="severity",
                barmode="group",
                text_auto=True,
                color_discrete_sequence=px.colors.qualitative.Safe,
                title="Alerts by Cause and Severity"
            )
            fig.update_layout(
                xaxis_title="Alert Cause",
                yaxis_title="Number of Alerts",
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                title_x=0.5,
                font=dict(size=13)
            )
            st.plotly_chart(fig, use_container_width=True)

            # --- Insight summary ---
            st.markdown("### 💡 Quick Insights")
            if not agg.empty:
                top_cause = agg.iloc[0]["cause"]
                top_severity = agg.iloc[0]["severity"]
                st.write(f"- The most frequent alert cause is **{top_cause}**, most often marked as **{top_severity}** severity.")
            if df["duration_hours"].notna().any():
                avg_duration = df["duration_hours"].mean()
                st.write(f"- The average alert lasted approximately **{avg_duration:.1f} hours**.")
            severe_alerts = df[df["severity"].astype(str).str.contains("Severe", case=False)]
            st.write(f"- Number of severe alerts: **{len(severe_alerts)}**")

            # --- Download button ---
            st.download_button("📥 Download Alerts Data", df.to_csv(index=False), "alerts_eda.csv")

    except Exception as e:
        st.error(f"alerts error: {e}")

# import streamlit as st
# import pandas as pd
# import plotly.express as px
# from google.cloud import bigquery

# PROJECT_ID = "christina-ba882-fall25"

# ===========================================================
# ------------------ BIGQUERY TABLE LOADER -------------------
# ===========================================================

@st.cache_data(ttl=120)
def load_table(table_name):
    """Loads a BigQuery table and handles errors safely."""
    try:
        client = bigquery.Client()
        query = f"SELECT * FROM `{PROJECT_ID}.mbta_core.{table_name}`"
        df = client.query(query).to_dataframe()

        if df.empty:
            st.warning(f"⚠ Table loaded but EMPTY: {table_name}")

        return df

    except Exception as e:
        st.error(f"❌ Failed to load `{table_name}`")
        st.exception(e)
        return pd.DataFrame()


# ===========================================================
# -------------------- LOAD DATASETS --------------------------
# ===========================================================

df_line  = load_table("mart_line_hour_delay")
df_route = load_table("mart_route_day_delay")
df_stop  = load_table("mart_stop_delay")

st.caption(
    f"Loaded → Lines: {len(df_line)}, Routes: {len(df_route)}, Stops: {len(df_stop)}"
)


# ===========================================================
# --------------------------- TABS ----------------------------
# ===========================================================

# tab_overview, tab_lines, tab_routes, tab_facilities, tab_predictions, tab_schedules, tab_shapes, tab_stops, tab_trips, tab_vehicles, tab_patterns, tab_alerts, tab_delay, tab_sql = st.tabs(
#     ["📊 Overview", "🧵 Lines", "🛤 Routes", "🏢 Facilities", "📈 Predictions", "🗓 Schedules", "🌀 Shapes", "🛑 Stops", "🚌 Trips", "🚗 Vehicles", "🗺 Patterns", "⚠ Alerts", "⏱ Delay Analytics", "🧪 SQL Lab"]
# )


# ===========================================================
# ----------- ALL DELAY ANALYTICS GOES INSIDE tab_delay -------
# ===========================================================

with tab_delay:

    st.header("⏱ Delay Analytics")
    st.caption("Powered by curated delay marts in `mbta_core`")

    level = st.radio(
        "Choose Delay Analytics Level:",
        ["Line", "Route", "Stop"],
        horizontal=True
    )

    def clean_delay(x):
        """Prevents unrealistic delays (e.g., -200,000 sec)."""
        if pd.isna(x):
            return 0
        if x < -10000:
            return 0
        return x


    # ===========================================================
    # ---------------------- LINE LEVEL ---------------------------
    # ===========================================================

    if level == "Line":

        st.subheader("🚇 Line-Level Delay Insights")

        if df_line.empty:
            st.warning("No line-level delay data available.")
            st.stop()

        df = df_line.copy()
        df["delay_clean"] = df["avg_delay_seconds"].fillna(0)

        df["line_name"] = df["line_id"].astype(str)

        avg_delay = df["delay_clean"].mean()
        worst_line = df.loc[df["delay_clean"].idxmin(), "line_name"]
        total_lines = df["line_name"].nunique()

        c2, c3 = st.columns(2)
        # c1.metric("Avg Delay (sec)", f"{avg_delay:,.1f}")
        c2.metric("Worst Line", worst_line)
        c3.metric("Lines Measured", total_lines)

        rank = (
            df.groupby("line_name")["delay_clean"]
            .mean()
            .reset_index()
            .sort_values("delay_clean")
        )

        fig = px.bar(rank, x="line_name", y="delay_clean",
                     title="Lines Ranked by Average Delay")
        st.plotly_chart(fig, use_container_width=True)

        fig2 = px.scatter(df, x="n_predictions", y="delay_clean",
                          color="line_name",
                          title="Delay vs Prediction Volume")
        st.plotly_chart(fig2, use_container_width=True)

        fig3 = px.box(df, x="line_name", y="delay_clean",
                      title="Delay Distribution Across Lines")
        st.plotly_chart(fig3, use_container_width=True)

        st.success("Line-level delay analytics ready.")



    # ===========================================================
    # ---------------------- ROUTE LEVEL --------------------------
    # ===========================================================
    # ===========================================================
# ---------------------- ROUTE LEVEL --------------------------
# ===========================================================

    if level == "Route":
        st.subheader("🛤 Route-Level Delay Insights")

        if df_route.empty:
            st.warning("Route table is empty.")
            st.stop()

        df = df_route.copy()

    # Clean delays
        df["delay_clean"] = df["avg_delay_seconds"].apply(clean_delay)

    # Use REAL readable names → line_id, NOT "True"
        df["route_name"] = df["line_id"].fillna(df["route_id"].astype(str))

    # --- Summary metrics ---
        # avg_delay = df["delay_clean"].mean()
        # worst_route = df.loc[df["delay_clean"].idxmin(), "route_name"]
        # total_routes = df["route_name"].nunique()

        # c1, c2, c3 = st.columns(3)
        # c1.metric("Avg Delay (sec)", f"{avg_delay:,.1f}")
        # c2.metric("Worst Route", worst_route)
        # c3.metric("Routes Measured", total_routes)

        # --- Summary metrics (MUCH more meaningful) ---

        # Typical delays
        median_delay = df["p50_delay_seconds"].median()          # typical rider delay
        p90_delay = df["p90_delay_seconds"].median()              # severe delay level
        pct_reliability = df["pct_trips_delayed_5min"].mean()     # avg % delayed >5min

        # Worst route = the one with the highest median delay
        worst_route = df.loc[df["p50_delay_seconds"].idxmax(), "route_name"]

        # Count of unique routes
        total_routes = df["route_name"].nunique()

        # Display metrics
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Typical Delay (Median)", f"{median_delay:,.0f} sec")
        c2.metric("Severe Delay (p90)", f"{p90_delay:,.0f} sec")
        c3.metric("% Trips Delayed >5 min", f"{pct_reliability:.1%}")
        c4.metric("Worst Route", worst_route)

        # Second row for route count
        st.metric("Routes Measured", total_routes)



    # ===========================================================
    # ⭐ SINGLE INSIGHTFUL PERFORMANCE CHART
    # ===========================================================

        st.markdown("### 📊 Reliability vs Delay Severity")

        fig = px.scatter(
            df,
            x="p50_delay_seconds",         # median delay
            y="pct_trips_delayed_5min",    # reliability
            size="n_predictions",          # bubble size = impact
            color="route_name",            # readable labels
            hover_data={
            "avg_delay_seconds": True,
            "p50_delay_seconds": True,
            "p90_delay_seconds": True,
            "n_predictions": True,
            "pct_trips_delayed_5min": True
            },
            title="Route Reliability vs Delay Severity",
            )

        fig.update_layout(
        xaxis_title="Median Delay (p50) — seconds",
        yaxis_title="% Trips Delayed > 5 min",
        legend_title="Route",
    )

        st.plotly_chart(fig, use_container_width=True)

        st.success("Route-level analytics ready.")

    
    # if level == "Route":
    #     st.subheader("🛤 Route-Level Delay Insights")
    #     if df_route.empty:
    #         st.warning("Route table is empty.")
    #         st.stop()
    #     df = df_route.copy()
        
    #     # FIX: use actual column names
    #     if "route_name" in df.columns:
    #         df["route_name"] = df["route_name"].fillna(df["route_id"].astype(str))
    #     else:
    #         df["route_name"] = df["route_id"].astype(str)
    #         df["delay_clean"] = df["avg_delay_seconds"].apply(clean_delay)
    #     avg_delay = df["delay_clean"].mean()
    #     worst_route = df.loc[df["delay_clean"].idxmin(), "route_name"]
    #     total_routes = df["route_name"].nunique()
        
    #     c1, c2, c3 = st.columns(3)
    #     c1.metric("Avg Delay (sec)", f"{avg_delay:,.1f}")
    #     c2.metric("Worst Route", worst_route)
    #     c3.metric("Routes Measured", total_routes)
        
    #     # Ranking
    #     rank = (
    #         df.groupby("route_name")["delay_clean"]
    #       .mean()
    #       .reset_index()
    #       .sort_values("delay_clean")
    # )

    #     fig = px.bar(rank, x="route_name", y="delay_clean",
    #              title="Routes Ranked by Average Delay")
    #     st.plotly_chart(fig, use_container_width=True)

    #     fig2 = px.scatter(df, x="n_predictions", y="delay_clean",
    #                   color="route_name",
    #                   title="Delay vs Number of Predictions")
    #     st.plotly_chart(fig2, use_container_width=True)

    #     fig3 = px.box(df, x="route_name", y="delay_clean",
    #               title="Delay Distribution per Route")
    #     st.plotly_chart(fig3, use_container_width=True)

    #     st.success("Route-level analytics ready.")




    # ===========================================================
    # ----------------------- STOP LEVEL --------------------------
    # ===========================================================

    if level == "Stop":

        st.subheader("🚏 Stop-Level Delay Insights")

        if df_stop.empty:
            st.warning("No stop-level delay data available.")
            st.stop()

        df = df_stop.copy()
        df["delay_clean"] = df["avg_delay_seconds"].fillna(0)

        df["stop_label"] = df["stop_name"] + " (ID " + df["stop_id"].astype(str) + ")"

        avg_delay = df["delay_clean"].mean()
        pct_delayed = df["pct_delayed_5min"].mean()

        (c2,) = st.columns(1)
        # c1.metric("Avg Network Delay", f"{avg_delay:,.1f} sec")
        c2.metric("Avg % Delayed >5 min", f"{pct_delayed:.1%}")

        rank = (
            df.groupby(["stop_id", "stop_name"])["delay_clean"]
            .mean()
            .reset_index()
            .sort_values("delay_clean")
        )

        fig = px.bar(rank.head(25), x="stop_name", y="delay_clean",
                     title="Stops Ranked by Average Delay (Top 25)")
        st.plotly_chart(fig, use_container_width=True)

        pct_rank = (
            df.groupby(["stop_id", "stop_name"])["pct_delayed_5min"]
            .mean()
            .reset_index()
            .sort_values("pct_delayed_5min", ascending=False)
        )

        fig2 = px.bar(pct_rank.head(25), x="stop_name", y="pct_delayed_5min",
                      title="% Trips Delayed >5 min (Top 25 Stops)")
        fig2.update_layout(yaxis_tickformat=".0%")
        st.plotly_chart(fig2, use_container_width=True)

        stop_options = df[["stop_id", "stop_name"]].drop_duplicates()
        stop_options["label"] = stop_options["stop_name"] + " (ID " + stop_options["stop_id"].astype(str) + ")"

        selected_label = st.selectbox("Choose Stop:", stop_options["label"])
        selected_id = stop_options.loc[
            stop_options["label"] == selected_label, "stop_id"
        ].iloc[0]

        stop_df = df[df["stop_id"] == selected_id]

        # fig3 = px.box(stop_df, y="delay_clean",
        #               title=f"Delay Distribution for Stop {selected_label}")
        # st.plotly_chart(fig3, use_container_width=True)

        st.subheader("Raw Data Preview")
        st.dataframe(stop_df, use_container_width=True)

        st.success("Stop-level delay analytics ready.")


# -------------------------- SQL LAB --------------------------
with tab_sql:
    st.subheader("Ad-hoc SQL Lab")
    st.caption(f"Project: `{PROJECT_ID}` · Dataset: `{dataset_name}`")
    example = f"SELECT * FROM `{PROJECT_ID}.{dataset_name}.lines` LIMIT 100"
    sql = st.text_area("SQL", value=example, height=160)
    run = st.button("▶ Run query")
    if run and sql.strip():
        try:
            df = client.query(sql).result().to_dataframe()
            st.success(f"Returned {len(df):,} rows.")
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.download_button("Download CSV", df.to_csv(index=False), "query_result.csv")
        except Exception as e:
            st.error(str(e))

st.success("✅ Dashboard Ready")

# # ===========================================================
# # 🤖 NEW TAB: ML Delay Predictor + AI Assistant
# # ===========================================================
# # ===========================================================
# # 🤖 NEW TAB: ML Delay Predictor + AI Assistant
# # ===========================================================
# with tab_ml:

#     st.header("🤖 ML Delay Predictor & AI Assistant")
#     st.caption("Uses your trained ML model stored in GCS + Vertex AI for natural queries.")

#     # ---------------- LOAD MODEL FROM GCS ----------------
#     @st.cache_resource
#     def load_delay_model():
#         from google.cloud import storage
#         import joblib
#         from io import BytesIO

#         bucket_name = "ba882-team10-bucket"
#         model_path = "models/mbta_delay_model.pkl"

#         client = storage.Client()
#         bucket = client.bucket(bucket_name)
#         blob = bucket.blob(model_path)

#         # Download raw bytes
#         model_bytes = blob.download_as_bytes()

#         # Load using joblib (correct for joblib-saved pkl)
#         model = joblib.load(BytesIO(model_bytes))
#         return model

#     with st.spinner("Loading ML delay model from GCS..."):
#         delay_model = load_delay_model()

#     st.success("Delay model loaded successfully ✔️")

#     # ---------------- STRUCTURED PREDICTION UI ----------------
#     st.subheader("📈 Predict Delay (Structured Inputs)")

#     if "route_name" not in df_route.columns:
#         st.error("df_route must contain 'route_name' column.")
#     else:
#         routes = sorted(df_route["route_name"].dropna().unique())

#         route_sel = st.selectbox("Choose Route:", routes)
#         hour_sel = st.slider("Hour of Day (0–23):", 0, 23, 8)
#         day_sel = st.selectbox(
#             "Day of Week:", 
#             ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
#         )

#         # Encoders — MUST match how model was trained
#         day_map = {"Mon":0, "Tue":1, "Wed":2, "Thu":3, "Fri":4, "Sat":5, "Sun":6}
#         route_map = {r: i for i, r in enumerate(routes)}

#         if st.button("🔮 Predict Delay"):
#             try:
#                 features = np.array([
#                     [hour_sel, day_map[day_sel], route_map[route_sel]]
#                 ])
#                 prediction = delay_model.predict(features)[0]
#                 st.metric("Predicted Delay (seconds)", f"{prediction:.2f}")
#             except Exception as e:
#                 st.error(f"Prediction failed: {e}")

#     # ---------------- AI NATURAL LANGUAGE QUERY ----------------
#     st.subheader("💬 Ask AI About Delays")

#     from vertexai.preview.generative_models import GenerativeModel

#     user_q = st.text_input(
#         "Ask something like 'predict delay for Red Line at 6pm on Friday'"
#     )

#     if user_q:
#         system_msg = """
#         Extract route (string), hour (0-23 integer), and weekday (Mon–Sun)
#         from the user's question about MBTA delays.
        
#         Output ONLY valid JSON like:
#         {"route": "Red Line", "hour": 17, "day": "Fri"}

#         If something is missing, guess reasonable defaults.
#         Never output extra text.
#         """

#         llm = GenerativeModel(
#             "gemini-2.5-flash",
#             system_instruction=system_msg
#         )

#         try:
#             raw = llm.generate_content(user_q).text

#             # Try reading JSON safely
#             try:
#                 params = json.loads(raw)
#             except:
#                 st.error("The AI did not return valid JSON.")
#                 st.write("AI output:", raw)
#                 st.stop()

#             # Extract fields with safe defaults
#             route = params.get("route", routes[0])
#             hour = int(params.get("hour", 12))
#             day = params.get("day", "Mon")

#             # Fix route if AI output is not exact match
#             if route not in route_map:
#                 # Try fuzzy matching or fallback to first
#                 route = routes[0]

#             # Prepare model features
#             features = np.array([
#                 [hour, day_map.get(day, 0), route_map[route]]
#             ])
#             pred = delay_model.predict(features)[0]

#             st.write("### 🤖 Estimated Delay")
#             st.success(f"{route} on {day} at {hour}:00 → **{pred:.2f} sec delay**")

#         except Exception as e:
#             st.error(f"AI Query Error: {e}")


