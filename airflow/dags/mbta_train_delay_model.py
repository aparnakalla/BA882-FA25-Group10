# dags/mbta_train_delay_model.py

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

# If you prefer PythonOperator, you can import it too
# from airflow.operators.python import PythonOperator

from include.train_delay_model import train_and_upload

TIMEZONE = "America/New_York"


@dag(
    dag_id="mbta_train_delay_model",
    start_date=pendulum.datetime(2025, 11, 1, tz=TIMEZONE),
    schedule="@daily",   # or None, and you trigger manually
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-science",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["mbta", "ml", "delay-model"],
)
def mbta_train_delay_model():
    """
    Train the MBTA delay prediction model from the mbta_ml.training_delay_stop
    table and upload the serialized pipeline to GCS.
    """

    @task
    def run_training():
        train_and_upload()

    run_training()


mbta_train_delay_model_dag = mbta_train_delay_model()
