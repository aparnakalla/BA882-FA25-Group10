# include/train_delay_model.py

import os
import logging

import pandas as pd
from google.cloud import bigquery, storage

from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report
from xgboost import XGBClassifier
import joblib


PROJECT_ID = os.environ.get("PROJECT_ID", "christina-ba882-fall25")
ML_DATASET = os.environ.get("ML_DATASET", "mbta_ml")
TRAINING_TABLE = os.environ.get("TRAINING_TABLE", "training_delay_stop")

MODEL_BUCKET = os.environ.get("MODEL_BUCKET", "ba882-team10-bucket")
MODEL_BLOB = os.environ.get("MODEL_BLOB", "models/mbta_delay_model.pkl")


def load_training_data() -> pd.DataFrame:
    client = bigquery.Client(project=PROJECT_ID)

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
    logging.info("Querying BigQuery training data...")
    df = client.query(query).to_dataframe()
    logging.info("Loaded %d rows from training table", len(df))
    return df


def build_features_and_target(df: pd.DataFrame):
    # Define label
    y = df["label_is_delayed_5min"].astype(int)

    # Define features (these should match what you used before)
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

    # Identify categorical vs numeric
    cat_cols = ["route_id", "line_id", "stop_id", "direction_id"]
    num_cols = ["stop_sequence", "hour_of_day", "day_of_week", "is_weekend"]

    return X, y, cat_cols, num_cols


def build_pipeline(cat_cols, num_cols):
    preprocess = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols),
            ("num", StandardScaler(), num_cols),
        ]
    )

    # Basic XGBoost config; you can tune these later
    clf = XGBClassifier(
        eval_metric="logloss",
        use_label_encoder=False,
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=3.0,  # adjust for imbalance
        random_state=42,
        n_jobs=-1,
    )

    model = Pipeline(steps=[
        ("preprocess", preprocess),
        ("clf", clf),
    ])

    return model


def train_model() -> Pipeline:
    df = load_training_data()
    X, y, cat_cols, num_cols = build_features_and_target(df)

    # Keep this simple: random split with stratify
    # (If you want, replace with a time-based split instead)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    model = build_pipeline(cat_cols, num_cols)

    logging.info("Fitting model...")
    model.fit(X_train, y_train)

    logging.info("Evaluating on held-out test set...")
    y_pred = model.predict(X_test)
    report = classification_report(y_test, y_pred)
    logging.info("Classification report:\n%s", report)

    return model


def upload_model_to_gcs(model: Pipeline):
    # Save locally
    local_path = "/tmp/mbta_delay_model.pkl"
    logging.info("Saving model locally to %s", local_path)
    joblib.dump(model, local_path)

    # Upload to GCS
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(MODEL_BUCKET)
    blob = bucket.blob(MODEL_BLOB)

    logging.info("Uploading model to gs://%s/%s", MODEL_BUCKET, MODEL_BLOB)
    blob.upload_from_filename(local_path)
    logging.info("Model upload complete.")


def train_and_upload():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting MBTA delay model training job")

    model = train_model()
    upload_model_to_gcs(model)

    logging.info("Training job finished successfully")
