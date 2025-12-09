# include/train_delay_model.py

from __future__ import annotations

import os
import logging

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report
from xgboost import XGBClassifier
import joblib

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

PROJECT_ID = os.environ.get("PROJECT_ID", "christina-ba882-fall25")

# Dataset / table where the training data lives
ML_DATASET = os.environ.get("ML_DATASET", "mbta_ml")
TRAINING_TABLE = os.environ.get("TRAINING_TABLE", "training_delay_stop")

# Where to save the trained model in GCS
MODEL_BUCKET = os.environ.get("MODEL_BUCKET", "ba882-team10-bucket")
MODEL_BLOB = os.environ.get("MODEL_BLOB", "models/mbta_delay_model.pkl")

# Airflow GCP connection id (same as you use for BigQuery operators)
GCP_CONN_ID = os.environ.get("GCP_BQ_CONN_ID", "google_cloud_default")


# ---------------------------------------------------------------------------
# DATA LOADING
# ---------------------------------------------------------------------------

def load_training_data() -> pd.DataFrame:
    """
    Pull training data from BigQuery using Airflow's BigQueryHook
    (so we use the same GCP connection / service account as the other DAGs).
    """
    query = f"""
        SELECT
          service_date,
          route_id,
          line_id,
          listed_route,
          trip_id,
          stop_id,
          stop_name,
          direction_id,
          stop_sequence,
          prediction_ts,
          hour_of_day,
          day_of_week,
          is_weekend,
          delay_seconds,
          label_is_delayed_5min
        FROM `{PROJECT_ID}.{ML_DATASET}.{TRAINING_TABLE}`
        WHERE delay_seconds IS NOT NULL
    """

    logging.info("Querying BigQuery training data via BigQueryHook...")
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)

    df = hook.get_pandas_df(sql=query)
    logging.info("Loaded %d rows from training table", len(df))

    return df


# ---------------------------------------------------------------------------
# FEATURE / TARGET BUILDING
# ---------------------------------------------------------------------------

def build_features_and_target(df: pd.DataFrame):
    """
    Build X, y and identify categorical vs numeric columns.
    This should mirror the feature logic you used in your notebook.
    """

    # Label: 1 if delay >= 5 min, 0 otherwise
    y = df["label_is_delayed_5min"].astype(int)

    # Feature columns (these must match what you'll use at inference time)
    feature_cols = [
        "route_id",
        "line_id",
        "stop_id",
        "direction_id",
        "stop_sequence",
        "hour_of_day",
        "day_of_week",
        "is_weekend",
    ]

    X = df[feature_cols].copy()

    cat_cols = ["route_id", "line_id", "stop_id", "direction_id"]
    num_cols = ["stop_sequence", "hour_of_day", "day_of_week", "is_weekend"]

    return X, y, cat_cols, num_cols


# ---------------------------------------------------------------------------
# MODEL PIPELINE
# ---------------------------------------------------------------------------

def build_pipeline(cat_cols, num_cols) -> Pipeline:
    """
    Build a sklearn Pipeline:
      - ColumnTransformer for preprocessing
      - XGBoost classifier
    """

    preprocess = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols),
            ("num", StandardScaler(), num_cols),
        ]
    )

    clf = XGBClassifier(
        eval_metric="logloss",
        use_label_encoder=False,
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=3.0,  # adjust for imbalance; you can tweak later
        random_state=42,
        n_jobs=-1,
    )

    model = Pipeline(steps=[
        ("preprocess", preprocess),
        ("clf", clf),
    ])

    return model


# ---------------------------------------------------------------------------
# TRAINING
# ---------------------------------------------------------------------------

def train_model() -> Pipeline:
    """
    Load data from BQ, split, train the model, print a classification report.
    Returns the fitted Pipeline.
    """
    df = load_training_data()
    if df.empty:
        raise RuntimeError("Training table returned 0 rows; cannot train model.")

    X, y, cat_cols, num_cols = build_features_and_target(df)

    # Simple stratified train/test split.
    # If you want a time-based split later, you can swap this out.
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    model = build_pipeline(cat_cols, num_cols)

    logging.info("Fitting delay model on %d training samples...", len(X_train))
    model.fit(X_train, y_train)

    logging.info("Evaluating on held-out test set (%d samples)...", len(X_test))
    y_pred = model.predict(X_test)
    report = classification_report(y_test, y_pred)
    logging.info("Classification report:\n%s", report)

    return model


# ---------------------------------------------------------------------------
# SAVE / UPLOAD MODEL
# ---------------------------------------------------------------------------

def upload_model_to_gcs(model: Pipeline):
    """
    Serialize the trained model to a local file and upload it to GCS
    via Airflow's GCSHook (so we reuse the same connection).
    """
    local_path = "/tmp/mbta_delay_model.pkl"
    logging.info("Saving model locally to %s", local_path)
    joblib.dump(model, local_path)

    logging.info("Uploading model to gs://%s/%s via GCSHook", MODEL_BUCKET, MODEL_BLOB)
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    gcs_hook.upload(
        bucket_name=MODEL_BUCKET,
        object_name=MODEL_BLOB,
        filename=local_path,
        mime_type="application/octet-stream",
    )

    logging.info("Model upload complete.")


# ---------------------------------------------------------------------------
# ENTRY POINT FOR DAG
# ---------------------------------------------------------------------------

def train_and_upload():
    """
    Top-level function the Airflow task calls.
    Trains the model and uploads the artifact to GCS.
    """
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting MBTA delay model training job")

    model = train_model()
    upload_model_to_gcs(model)

    logging.info("Training job finished successfully")


# Optional: allow running locally for debugging
if __name__ == "__main__":
    train_and_upload()
